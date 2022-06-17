// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use log::debug;
use p2panda_rs::storage_provider::traits::OperationStore;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::Shutdown;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::worker::{Factory, Task, TaskStatus};
use crate::materializer::TaskInput;

/// Capacity of the internal broadcast channels used inside the worker factory.
///
/// This gives an upper bound to maximum status messages and incoming tasks being moved into worker
/// queues the channels can handle at once.
const CHANNEL_CAPACITY: usize = 1024;

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
    let mut on_update = factory.on_update();
    let store = context.store.clone();

    // Keep track of status changes and persist it in the database. This allows us to pick up
    // uncompleted tasks next time we start the node.
    let status_handle = task::spawn(async move {
        loop {
            match on_update.recv().await {
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
                .expect(&format!(
                    "Failed database query when retreiving document for operation_id {}",
                    operation_id
                )) {
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
