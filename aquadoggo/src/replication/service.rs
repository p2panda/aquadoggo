use std::time::Duration;

use anyhow::Result;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::graphql::replication::client;
use crate::manager::Shutdown;
use graphql_client::*;
use tokio::task;

pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
) -> Result<()> {
    // Things this needs to do
    // - get the ips of remotes who we connect to (comes from config)
    // - get authors we wish to replicate (comes from config)
    // - get the authors latest sequences for the log ids we follow
    // - for each author and each log_id
    //  - get their latest, paging through until has_next_page is false
    // - append new entries to the db
    // - broadcast a notification on the bus?

    let handle = task::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });
    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    Ok(())
}
