// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::Shutdown;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::worker::{Factory, Task};
use crate::materializer::TaskInput;

const CHANNEL_CAPACITY: usize = 1024;

pub async fn materializer_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
) -> Result<()> {
    // Create worker factory with task queue
    let pool_size = context.config.worker_pool_size as usize;
    let mut factory = Factory::<TaskInput, Context>::new(context, CHANNEL_CAPACITY);

    // Register worker functions in factory
    factory.register("reduce", pool_size, reduce_task);
    factory.register("dependency", pool_size, dependency_task);
    factory.register("schema", pool_size, schema_task);

    // Get a listener for error signal from factory
    let on_error = factory.on_error();

    // Subscribe to communication bus
    let mut rx = tx.subscribe();

    // Listen to incoming new entries and operations and move them into task queue
    let handle = task::spawn(async move {
        while let Ok(ServiceMessage::NewEntryAndOperation(entry, operation)) = rx.recv().await {
            // @TODO: Identify document id from operation by asking the database
            factory.queue(Task::new("reduce", TaskInput::new(None, None)));
        }
    });

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
        _ = on_error => (),
    }

    Ok(())
}
