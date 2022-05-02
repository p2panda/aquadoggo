// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::Shutdown;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::worker::Factory;

const CHANNEL_CAPACITY: usize = 1024;

// @TODO
pub type Input = usize;

pub async fn materializer_service(
    context: Context,
    shutdown: Shutdown,
    _tx: ServiceSender,
) -> Result<()> {
    let pool_size = context.config.worker_pool_size as usize;
    let mut factory = Factory::<Input, Context>::new(context, CHANNEL_CAPACITY);

    factory.register("reduce", pool_size, reduce_task);
    factory.register("dependency", pool_size, dependency_task);
    factory.register("schema", pool_size, schema_task);

    // Wait until we received the application shutdown signal
    shutdown.await?;

    Ok(())
}
