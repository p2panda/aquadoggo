use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use graphql_client::*;
use log::{info, trace};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use serde::Deserialize;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::graphql::replication::client::{self, Client};
use crate::manager::{Service, Shutdown};

#[derive(Default, Debug, Clone, Deserialize)]
pub struct Config {
    connection_interval_seconds: Option<u64>,
    remote_peers: Vec<String>,
    authors_to_replicate: Vec<(Author, Vec<LogId>)>,
}

#[derive(Default, Debug, Clone)]
pub struct ReplicationService {
    config: Config,
}

impl ReplicationService {
    pub fn new(config: Config) -> Self {
        trace!("init ReplicationService with config: {:?}", config);
        Self { config }
    }
}

#[async_trait::async_trait]
impl Service<Context, ServiceMessage> for ReplicationService {
    async fn call(&self, context: Context, shutdown: Shutdown, tx: ServiceSender) -> Result<()> {
        // Things this needs to do
        // - get the ips of remotes who we connect to (comes from config)
        // - get authors we wish to replicate (comes from config)
        // - get the authors latest sequences for the log ids we follow
        // - for each author and each log_id
        //  - get their latest, paging through until has_next_page is false
        // - append new entries to the db
        // - broadcast a notification on the bus?

        let connection_interval = self
            .config
            .connection_interval_seconds
            .map(|s| Duration::from_secs(s))
            .unwrap_or_else(|| Duration::from_secs(30));

        let mut client = Client::new();
        let authors_to_replicate = Arc::new(self.config.authors_to_replicate.clone());
        let remote_peers = Arc::new(self.config.remote_peers.clone());

        let handle = task::spawn(async move {
            loop {
                trace!("Starting replication with remote peers");
                for remote_peer in remote_peers.clone().iter() {
                    for (author, log_ids) in authors_to_replicate.clone().iter() {
                        for log_id in log_ids {
                            let entries = client
                                .get_entries_newer_than_seq(
                                    remote_peer,
                                    &log_id,
                                    &author,
                                    &SeqNum::new(1).unwrap(),
                                )
                                .await;
                            trace!("received entries: {:?}", entries);
                        }
                    }
                }
                tokio::time::sleep(connection_interval).await;
            }
        });

        tokio::select! {
            _ = handle => (),
            _ = shutdown => (trace!("shutdown")),
        }

        Ok(())
    }
}
