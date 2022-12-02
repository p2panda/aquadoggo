// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use log::{debug, warn};
use p2panda_rs::storage_provider::traits::OperationStore;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, ServiceStatusSender, Shutdown};
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::worker::{Factory, Task, TaskStatus};
use crate::materializer::TaskInput;
use crate::bus::ServiceStatusMessage;

/// Capacity of the internal broadcast channels used inside the worker factory.
///
/// This gives an upper bound to maximum status messages and incoming tasks being moved into worker
/// queues the channels can handle at once.
const CHANNEL_CAPACITY: usize = 512_000;

/// The materializer service waits for incoming new operations to transform them into actual useful
/// application- and system data, like document views or schemas.
///
/// Internally the service uses a task queue which gives us the right architecture to deal with
/// operations coming in random order and avoid race-conditions which would occur otherwise when
/// working on the same data in separate threads.
pub async fn materializer_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
    tx_status: ServiceStatusSender<ServiceStatusMessage>,
) -> Result<()> {
    // Create worker factory with task queue
    let pool_size = context.config.worker_pool_size as usize;
    let mut factory = Factory::<TaskInput, Context>::new(context.clone(), CHANNEL_CAPACITY);

    // Register worker functions in factory
    factory.register("reduce", pool_size, reduce_task);
    factory.register("dependency", pool_size, dependency_task);
    factory.register("schema", pool_size, schema_task);

    // Get a listener for error signal from factory
    let on_error = factory.on_error();

    // Subscribe to status changes of tasks
    let mut on_task_status_change = factory.on_task_status_change();
    let store = context.store.clone();

    // Keep track of status changes and persist it in the database. This allows us to pick up
    // uncompleted tasks next time we start the node.
    let status_handle = task::spawn(async move {
        loop {
            match on_task_status_change.recv().await {
                Ok(TaskStatus::Pending(task)) => {
                    store
                        .insert_task(&task)
                        .await
                        .expect("Failed inserting pending task into database");

                    // Send message on the service status channel
                    let _ = tx_status.send(ServiceStatusMessage::Materialiser(
                        TaskStatus::Pending(task),
                    ));
                }
                Ok(TaskStatus::Completed(task)) => {
                    store
                        .remove_task(&task)
                        .await
                        .expect("Failed removing completed task from database");

                    // Send message on the service status channel
                    let _ = tx_status.send(ServiceStatusMessage::Materialiser(
                        TaskStatus::Completed(task),
                    ));
                }
                Err(err) => {
                    panic!("Failed receiving task status updates: {}", err)
                }
            }
        }
    });

    // Reschedule tasks from last time which did not complete
    let tasks = context
        .store
        .get_tasks()
        .await
        .expect("Failed retreiving pending tasks from database");

    debug!("Dispatch {} pending tasks from last runtime", tasks.len());

    tasks.iter().for_each(|task| {
        factory.queue(task.to_owned());
    });

    // Subscribe to communication bus
    let mut rx = tx.subscribe();

    // Listen to incoming new entries and operations and move them into task queue
    let handle = task::spawn(async move {
        while let Ok(ServiceMessage::NewOperation(operation_id)) = rx.recv().await {
            // Resolve document id of regarding operation
            match context
                .store
                .get_document_by_operation_id(&operation_id)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed database query when retreiving document for operation_id {}",
                        operation_id
                    )
                }) {
                Some(document_id) => {
                    // Dispatch "reduce" task which will materialize the regarding document
                    factory.queue(Task::new("reduce", TaskInput::new(Some(document_id), None)));
                }
                None => {
                    // Panic when we couldn't find the regarding document in the database. We can
                    // safely assure that this is due to a critical bug affecting the database
                    // integrity. Panicking here will close `handle` and by that signal a node
                    // shutdown.
                    panic!("Could not find document for operation_id {}", operation_id);
                }
            }
        }
    });

    debug!("Materialiser service is ready");
    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about materialiser service being ready");
    };

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = status_handle => (),
        _ = shutdown => {
            // @TODO: Wait until all pending tasks have been completed during graceful shutdown.
            // Related issue: https://github.com/p2panda/aquadoggo/issues/164
        },
        _ = on_error => (),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationId, OperationValue};
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use p2panda_rs::test_utils::db::test_db::send_to_store;
    use p2panda_rs::test_utils::fixtures::{key_pair, operation, operation_fields, schema};
    use rstest::rstest;
    use tokio::sync::{broadcast, oneshot};
    use tokio::task;

    use crate::context::Context;
    use crate::db::stores::test_utils::{
        doggo_fields, doggo_schema, test_db, TestDatabase, TestDatabaseRunner,
    };
    use crate::materializer::{Task, TaskInput, TaskStatus};
    use crate::bus::ServiceStatusMessage;
    use crate::schema::SchemaProvider;
    use crate::Configuration;

    use super::materializer_service;

    #[rstest]
    fn materialize_document_from_bus(
        #[from(test_db)]
        #[with(1, 1, 1, false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        runner: TestDatabaseRunner,
    ) {
        // Prepare database which inserts data for one document
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Identify document and operation which was inserted for testing
            let document_id = db.test_data.documents.first().unwrap();

            // We can infer the id of the first operation from the document id
            let first_operation_id: OperationId = document_id.to_string().parse().unwrap();

            // We expect that the database does not contain any materialized document yet
            assert!(db
                .store
                .get_document_by_id(document_id)
                .await
                .unwrap()
                .is_none());

            // Prepare arguments for service
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _) = broadcast::channel(1024);
            let (tx_status, mut rx_status) = broadcast::channel(1024);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready, tx_status)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(first_operation_id))
                .unwrap();

            // Wait for the document to be materialised.
            while !matches!(
                rx_status.recv().await.unwrap(),
                ServiceStatusMessage::Materialiser(TaskStatus::Completed(_))
            ) {}

            // Make sure the service did not crash and is still running
            assert_eq!(handle.is_finished(), false);

            // Check database for materialized documents
            let document = db
                .store
                .get_document_by_id(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id().to_string(), document_id.to_string());
            assert_eq!(
                document.fields().get("name").unwrap().value().to_owned(),
                OperationValue::String("panda".into())
            );
        });
    }

    #[rstest]
    fn materialize_document_from_last_runtime(
        #[from(test_db)]
        #[with(1, 1, 1, false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        runner: TestDatabaseRunner,
    ) {
        // Prepare database which inserts data for one document
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Identify document and operation which was inserted for testing
            let document_id = db.test_data.documents.first().unwrap();

            // Store a pending "reduce" task from last runtime in the database so it gets picked up by
            // the materializer service
            db.store
                .insert_task(&Task::new(
                    "reduce",
                    TaskInput::new(Some(document_id.to_owned()), None),
                ))
                .await
                .unwrap();

            // Prepare arguments for service
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _) = broadcast::channel(1024);
            let (tx_status, mut rx_status) = broadcast::channel(1024);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready, tx_status)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Wait for the document to be materialised.
            while !matches!(
                rx_status.recv().await.unwrap(),
                ServiceStatusMessage::Materialiser(TaskStatus::Completed(_))
            ) {}

            // Make sure the service did not crash and is still running
            assert_eq!(handle.is_finished(), false);

            // Check database for materialized documents
            let document = db
                .store
                .get_document_by_id(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id().to_string(), document_id.to_string());
            assert_eq!(
                document.fields().get("name").unwrap().value().to_owned(),
                OperationValue::String("panda".into())
            );
        });
    }

    #[rstest]
    fn materialize_update_document(
        #[from(test_db)]
        #[with(1, 1, 1, false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        runner: TestDatabaseRunner,
    ) {
        // Prepare database which inserts data for one document
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Identify key_pair, document and operation which was inserted for testing
            let key_pair = db.test_data.key_pairs.first().unwrap();
            let document_id = db.test_data.documents.first().unwrap();

            // We can infer the id of the first operation from the document id
            let first_operation_id: OperationId = document_id.to_string().parse().unwrap();

            // Prepare arguments for service
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _) = broadcast::channel(1024);
            let (tx_status, mut rx_status) = broadcast::channel(1024);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready, tx_status)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(
                first_operation_id.clone(),
            ))
            .unwrap();

            // Wait for the document to be materialised.
            while !matches!(
                rx_status.recv().await.unwrap(),
                ServiceStatusMessage::Materialiser(TaskStatus::Completed(_))
            ) {}

            // Then straight away publish an UPDATE on this document and send it over the bus too.
            let (entry_encoded, _) = send_to_store(
                &db.store,
                &operation(
                    Some(operation_fields(vec![(
                        "name",
                        OperationValue::String("panda123".into()),
                    )])),
                    Some(first_operation_id.clone().into()),
                    SCHEMA_ID.parse().unwrap(),
                ),
                &schema(
                    vec![("name".to_string(), FieldType::String)],
                    SCHEMA_ID.parse().unwrap(),
                    "A test schema",
                ),
                key_pair,
            )
            .await
            .expect("Publish entry");

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(
                entry_encoded.hash().into(),
            ))
            .unwrap();

            // Wait for the document to be materialised.
            while !matches!(
                // A "dependency" task gets completed first so we add a condition to
                // the match
                rx_status.recv().await.unwrap(),
                ServiceStatusMessage::Materialiser(
                    TaskStatus::Completed(task)
                ) if task.worker_name() == "reduce"
            ) {}

            // Make sure the service did not crash and is still running
            assert_eq!(handle.is_finished(), false);

            // Check database for materialized documents
            let document = db
                .store
                .get_document_by_id(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id(), &entry_encoded.hash().into());
            assert_eq!(
                document.fields().get("name").unwrap().value().to_owned(),
                OperationValue::String("panda123".into())
            );
        });
    }

    #[rstest]

    fn materialize_complex_documents(
        #[from(test_db)]
        #[with(0, 0, 0)]
        runner: TestDatabaseRunner,
        #[from(operation)]
        #[with(Some(operation_fields(doggo_fields())), None, doggo_schema().id().to_owned())]
        operation: Operation,
        key_pair: KeyPair,
    ) {
        // Prepare empty database
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Prepare arguments for service
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _rx) = broadcast::channel(1024);

            // Start materializer service
            let tx_clone = tx.clone();
            let (tx_status, mut rx_status) = broadcast::channel(1024);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready, tx_status)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Then straight away publish a CREATE operation and send it to the bus.
            let (entry_encoded, _) =
                send_to_store(&db.store, &operation, &doggo_schema(), &key_pair)
                    .await
                    .expect("Publish entry");

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(
                p2panda_rs::entry::traits::AsEncodedEntry::hash(&entry_encoded).into(),
            ))
            .unwrap();

            // Wait for the document to be materialised.
            while !matches!(
                rx_status.recv().await.unwrap(),
                ServiceStatusMessage::Materialiser(TaskStatus::Completed(_))
            ) {}

            // Make sure the service did not crash and is still running
            assert_eq!(handle.is_finished(), false);

            // Check database for materialized documents
            let document = db
                .store
                .get_document_by_id(&entry_encoded.hash().into())
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id(), &entry_encoded.hash().into());
        });
    }
}
