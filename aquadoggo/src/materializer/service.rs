// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use log::{debug, warn};
use p2panda_rs::storage_provider::traits::OperationStore;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::materializer::tasks::{
    blob_task, dependency_task, garbage_collection_task, reduce_task, schema_task,
};
use crate::materializer::worker::{Factory, Task, TaskStatus};
use crate::materializer::TaskInput;

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
) -> Result<()> {
    // Create worker factory with task queue
    let pool_size = context.config.worker_pool_size as usize;
    let mut factory = Factory::<TaskInput, Context>::new(context.clone(), CHANNEL_CAPACITY);

    // Register worker functions in factory
    factory.register("reduce", pool_size, reduce_task);
    factory.register("dependency", pool_size, dependency_task);
    factory.register("schema", pool_size, schema_task);
    factory.register("blob", pool_size, blob_task);
    factory.register("garbage_collection", pool_size, garbage_collection_task);

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
                }
                Ok(TaskStatus::Completed(task)) => {
                    store
                        .remove_task(&task)
                        .await
                        .expect("Failed removing completed task from database");
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
        .expect("Failed retrieving pending tasks from database");

    debug!("Dispatch {} pending tasks from last runtime", tasks.len());

    tasks.iter().for_each(|task| {
        factory.queue(task.to_owned());
    });

    let handle = {
        let context = context.clone();

        // Subscribe to communication bus
        let mut rx = tx.subscribe();

        // Listen to incoming new entries and operations and move them into task queue
        task::spawn(async move {
            loop {
                if let Ok(ServiceMessage::NewOperation(operation_id)) = rx.recv().await {
                    // Resolve document id of regarding operation
                    let document_id = context
                        .store
                        .get_document_id_by_operation_id(&operation_id)
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                            "Failed database query when retrieving document id by operation_id {}",
                            operation_id
                        )
                        });

                    match document_id {
                        Some(document_id) => {
                            // Dispatch "reduce" task which will materialize the regarding document.
                            factory.queue(Task::new("reduce", TaskInput::DocumentId(document_id)))
                        }
                        None => {
                            // Panic when we couldn't find the regarding document in the database. We can
                            // safely assure that this is due to a critical bug affecting the database
                            // integrity. Panicking here will close `handle` and by that signal a node
                            // shutdown.
                            panic!("Could not find document for operation_id {}", operation_id);
                        }
                    };
                }
            }
        })
    };

    debug!("Materialiser service is ready");
    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about materialiser service being ready");
    };

    // Re-apply unmaterialized operations as they might have slipped through in an unexpected crash
    // or node shutdown
    let unindexed_operation_ids = context
        .store
        .get_unindexed_operation_ids()
        .await
        .unwrap_or_else(|_| panic!("Failed database query when loading unindexed operation ids"));

    for id in unindexed_operation_ids {
        let _ = tx.send(ServiceMessage::NewOperation(id));
    }

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = status_handle => (),
        _ = shutdown => (),
        _ = on_error => (),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationId, OperationValue};
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{key_pair, operation, operation_fields, schema};
    use p2panda_rs::test_utils::memory_store::helpers::send_to_store;
    use rstest::rstest;
    use tokio::sync::{broadcast, oneshot};
    use tokio::task;

    use crate::context::Context;
    use crate::materializer::{Task, TaskInput};
    use crate::schema::SchemaProvider;
    use crate::test_utils::{
        doggo_fields, doggo_schema, populate_store, populate_store_config, test_runner,
        PopulateStoreConfig, TestNode,
    };
    use crate::Configuration;

    use super::materializer_service;

    #[rstest]
    fn materialize_document_from_bus(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()], false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        config: PopulateStoreConfig,
    ) {
        // Prepare database which inserts data for one document
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents
            let documents = populate_store(&node.context.store, &config).await;
            let document_id = documents[0].id();

            // We can infer the id of the first operation from the document id
            let first_operation_id: OperationId = document_id.to_string().parse().unwrap();

            // We expect that the database does not contain any materialized document yet
            assert!(node
                .context
                .store
                .get_document(document_id)
                .await
                .unwrap()
                .is_none());

            // Prepare arguments for service
            let context = Context::new(
                node.context.store.clone(),
                KeyPair::new(),
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
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(first_operation_id))
                .unwrap();

            // Wait a little bit for work being done ..
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Make sure the service did not crash and is still running
            assert!(!handle.is_finished());

            // Check database for materialized documents
            let document = node
                .context
                .store
                .get_document(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id().to_string(), document_id.to_string());
            assert_eq!(
                document.get("name").unwrap().to_owned(),
                OperationValue::String("panda".into())
            );
        });
    }

    #[rstest]
    fn materialize_document_from_last_runtime(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()], false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents
            let documents = populate_store(&node.context.store, &config).await;
            let document_id = documents[0].id();

            // Store a pending "reduce" task from last runtime in the database so it gets picked up
            // by the materializer service
            node.context
                .store
                .insert_task(&Task::new(
                    "reduce",
                    TaskInput::DocumentId(document_id.to_owned()),
                ))
                .await
                .unwrap();

            // Prepare arguments for service
            let context = Context::new(
                node.context.store.clone(),
                KeyPair::new(),
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
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Wait for service to be done .. it should materialize the document since it was
            // waiting as a "pending" task in the database
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Make sure the service did not crash and is still running
            assert!(!handle.is_finished());

            // Check database for materialized documents
            let document = node
                .context
                .store
                .get_document(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id().to_string(), document_id.to_string());
            assert_eq!(
                document.get("name").unwrap().to_owned(),
                OperationValue::String("panda".into())
            );
        });
    }

    #[rstest]
    fn materialize_unhandled_operations(
        #[from(operation)]
        #[with(Some(operation_fields(doggo_fields())), None, doggo_schema().id().to_owned())]
        operation: Operation,
        key_pair: KeyPair,
    ) {
        test_runner(move |node: TestNode| async move {
            // Prepare arguments for service
            let context = Context::new(
                node.context.store.clone(),
                KeyPair::new(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            // Create an operation in the database which was not handled by the `reduce` task yet.
            // This might happen for example when the node crashed right _after_ the operation
            // arrived in the database but _before_ the `reduce` task kicked in.
            let (entry_signed, _) =
                send_to_store(&node.context.store, &operation, &doggo_schema(), &key_pair)
                    .await
                    .expect("Publish CREATE operation");
            let document_id: DocumentId = entry_signed.hash().into();

            // There should be one unhandled operation in the database
            let unindexed_operation_ids = context
                .store
                .get_unindexed_operation_ids()
                .await
                .unwrap_or_else(|_| {
                    panic!("Failed database query when loading unindexed operation ids")
                });
            assert!(unindexed_operation_ids.len() == 1);

            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _) = broadcast::channel(1024);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service, it should pick up the un-indexed operation automatically
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Wait for service to be done .. it should materialize the document since it was
            // waiting as a "pending" task in the database
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Make sure the service did not crash and is still running
            assert!(!handle.is_finished());

            // Check database for materialized documents
            let document = node
                .context
                .store
                .get_document(&document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id().to_string(), document_id.to_string());
            assert_eq!(
                document.get("username").unwrap().to_owned(),
                OperationValue::String("bubu".into())
            );
        });
    }

    #[rstest]
    fn materialize_update_document(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()], false, schema(vec![("name".to_string(), FieldType::String)], SCHEMA_ID.parse().unwrap(), "A test schema"), vec![("name", OperationValue::String("panda".into()))])]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document_id = documents[0].id();
            let key_pair = &config.authors[0];

            // We can infer the id of the first operation from the document id
            let first_operation_id: OperationId = document_id.to_string().parse().unwrap();

            // Prepare arguments for service
            let context = Context::new(
                node.context.store.clone(),
                KeyPair::new(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let shutdown = task::spawn(async {
                loop {
                    // Do this forever .. this means that the shutdown handler will never resolve
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let (tx, _) = broadcast::channel(128);
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            // Start materializer service
            let tx_clone = tx.clone();
            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready)
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

            // Wait a little bit for work being done ..
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Then straight away publish an UPDATE on this document and send it over the bus too.
            let (entry_encoded, _) = send_to_store(
                &node.context.store,
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

            // Wait a little bit for work being done ..
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Make sure the service did not crash and is still running
            assert!(!handle.is_finished());

            // Check database for materialized documents
            let document = node
                .context
                .store
                .get_document(document_id)
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");

            assert_eq!(document.id(), document_id);
            assert_eq!(
                document.get("name").unwrap().to_owned(),
                OperationValue::String("panda123".into())
            );
        });
    }

    #[rstest]
    fn materialize_complex_documents(
        #[from(operation)]
        #[with(Some(operation_fields(doggo_fields())), None, doggo_schema().id().to_owned())]
        operation: Operation,
        key_pair: KeyPair,
    ) {
        test_runner(move |node: TestNode| async move {
            // Prepare arguments for service
            let context = Context::new(
                node.context.store.clone(),
                KeyPair::new(),
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
            let (tx_ready, rx_ready) = oneshot::channel::<()>();

            let handle = tokio::spawn(async move {
                materializer_service(context, shutdown, tx_clone, tx_ready)
                    .await
                    .unwrap();
            });

            if rx_ready.await.is_err() {
                panic!("Service dropped");
            }

            // Then straight away publish a CREATE operation and send it to the bus.
            let (entry_encoded, _) =
                send_to_store(&node.context.store, &operation, &doggo_schema(), &key_pair)
                    .await
                    .expect("Publish entry");

            // Send a message over the bus which kicks in materialization
            tx.send(crate::bus::ServiceMessage::NewOperation(
                AsEncodedEntry::hash(&entry_encoded).into(),
            ))
            .unwrap();

            // Wait a little bit for work being done..
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Make sure the service did not crash and is still running
            assert!(!handle.is_finished());

            // Check database for materialized documents
            let document = node
                .context
                .store
                .get_document(&entry_encoded.hash().into())
                .await
                .unwrap()
                .expect("We expect that the document is `Some`");
            assert_eq!(document.id(), &entry_encoded.hash().into());
        });
    }
}
