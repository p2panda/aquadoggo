// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use log::{debug, error, warn};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::storage_provider::traits::EntryStore;
use serde::Deserialize;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::stores::StorageEntry;
use crate::graphql::replication::client::Client;
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
        debug!("init ReplicationService with config: {:?}", config);
        Self { config }
    }
}

#[async_trait::async_trait]
impl Service<Context, ServiceMessage> for ReplicationService {
    async fn call(&self, context: Context, shutdown: Shutdown, tx: ServiceSender) -> Result<()> {
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
                debug!("Starting replication with remote peers");
                for remote_peer in remote_peers.clone().iter() {
                    for (author, log_ids) in authors_to_replicate.clone().iter() {
                        for log_id in log_ids {

                            // Get the latest seq we have for this log + author
                            let latest_seq = get_latest_seq(&context, &log_id, &author).await;
                            debug!("Latest entry seq: {:?}", latest_seq);

                            // Make our replication request to the remote peer
                            let entries = client
                                .get_entries_newer_than_seq(
                                    remote_peer,
                                    &log_id,
                                    &author,
                                    latest_seq.as_ref(),
                                )
                                .await;

                            if let Ok(entries) = entries {
                                debug!("Received {} new entries", entries.len());

                                // TODO: verify entries

                                for entry in entries {
                                    context
                                        .0
                                        .store
                                        .insert_entry(entry.clone())
                                        .await
                                        .map(|_| {
                                            send_new_entry_service_message(tx.clone(), &entry);
                                        })
                                        .unwrap_or_else(|err| {
                                            error!(
                                                "Failed to insert entry: {:?}, err: {:?}",
                                                entry, err
                                            );
                                        });
                                }
                            } else {
                                warn!("Replication request failed");
                            }
                        }
                    }
                }
                tokio::time::sleep(connection_interval).await;
            }
        });

        tokio::select! {
            _ = handle => (),
            _ = shutdown => (debug!("shutdown")),
        }

        Ok(())
    }
}

fn send_new_entry_service_message(tx: ServiceSender, entry: &StorageEntry) {
    let bus_message = ServiceMessage::NewEntryAndOperation(
        entry.entry_decoded(),
        entry.operation_encoded().unwrap().into(),
    );
    tx.send(bus_message)
        .expect("Expected to be able to send a ServiceMessage on ServiceSender");
}

async fn get_latest_seq(context: &Context, log_id: &LogId, author: &Author) -> Option<SeqNum> {
    context
        .0
        .store
        .get_latest_entry(&author, &log_id)
        .await
        .ok()
        .flatten()
        .map(|entry| entry.entry_decoded().seq_num().clone())
}
