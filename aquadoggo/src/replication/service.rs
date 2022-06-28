// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use bamboo_rs_core_ed25519_yasmf::verify::verify_batch;
use futures::TryFutureExt;
use log::{debug, error, trace, warn};
pub use p2panda_rs::entry::LogId;
use p2panda_rs::entry::SeqNum;
pub use p2panda_rs::identity::Author;
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
use serde::Deserialize;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::stores::StorageEntry;
use crate::graphql::replication::client::Client;
use crate::manager::{Service, Shutdown};

#[derive(Debug, Clone, Deserialize)]
pub struct AuthorToReplicate(Author, Vec<LogId>);

impl TryFrom<(String, Vec<u64>)> for AuthorToReplicate {
    type Error = Error;

    fn try_from(value: (String, Vec<u64>)) -> Result<Self, Self::Error> {
        let author = Author::new(&value.0)?;
        let log_ids = value.1.into_iter().map(LogId::new).collect();
        Ok(Self(author, log_ids))
    }
}

/// Configuration for the replication service
#[derive(Default, Debug, Clone, Deserialize)]
pub struct Config {
    /// How often to connect to remote nodes for replication.
    pub connection_interval_seconds: Option<u64>,
    /// The addresses of remote peers to replicate from.
    pub remote_peers: Vec<String>,
    /// The authors to replicate and their log ids.
    pub authors_to_replicate: Vec<AuthorToReplicate>,
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
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));

        let mut client = Client::new();
        let authors_to_replicate = Arc::new(self.config.authors_to_replicate.clone());
        let remote_peers = Arc::new(self.config.remote_peers.clone());

        let handle = task::spawn(async move {
            loop {
                debug!("Starting replication with remote peers");
                for remote_peer in remote_peers.clone().iter() {
                    for author_to_replicate in authors_to_replicate.clone().iter() {
                        let author = author_to_replicate.0.clone();
                        let log_ids = author_to_replicate.1.clone();

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

                                if verify_entries(&entries, &context).await.is_err() {
                                    warn!("couldn't verify entries");
                                    continue;
                                }

                                insert_new_entries(&entries, &context, tx.clone())
                                    .await
                                    .unwrap_or_else(|e| error!("{:?}", e));
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

async fn verify_entries(entries: &[StorageEntry], context: &Context) -> Result<()> {
    debug!("Received {} new entries", entries.len());

    // Get the first entry (assumes they're sorted by seq_num smallest
    // to largest)
    let first_entry = entries.get(0).cloned();
    let mut entries_to_verify = entries.to_vec();

    match first_entry.as_ref() {
        // If the entry is the first in the log then we can don't need
        // to attempt to get the skiplink and previous
        Some(entry) if entry.seq_num() == SeqNum::new(1).unwrap() => {
            trace!("first entry had seq_num 1 do no need to get previous entries in db");
        }
        Some(entry) => {
            trace!("getting cert pool for entries");
            add_certpool_to_entries_for_verification(&mut entries_to_verify, entry, context)
                .await?;
        }
        None => (),
    }

    let entries_to_verify: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries_to_verify
        .iter()
        .map(|entry| (entry.entry_bytes(), None))
        .collect();

    verify_batch(&entries_to_verify)?;

    Ok(())
}

async fn insert_new_entries(
    new_entries: &[StorageEntry],
    context: &Context,
    tx: ServiceSender,
) -> Result<()> {
    let futures = new_entries.iter().map(|entry| {
        context
            .0
            .store
            .insert_entry(entry.clone())
            .map_ok({
                let entry = entry.clone();
                let tx = tx.clone();
                move |_| {
                    send_new_entry_service_message(tx.clone(), &entry);
                }
            })
            .map_err(|err| anyhow!(format!("error inserting new entry into db: {:?}", err)))
    });

    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect::<Result<_>>()?;

    Ok(())
}

async fn add_certpool_to_entries_for_verification(
    entries: &mut Vec<StorageEntry>,
    first_entry: &StorageEntry,
    context: &Context,
) -> Result<()> {
    trace!("getting cert pool for entries");
    let mut certpool = context
        .0
        .store
        .get_certificate_pool(
            &first_entry.author(),
            &first_entry.log_id(),
            &first_entry.seq_num(),
        )
        .await?;

    trace!("got {} certpool entries", certpool.len());
    entries.append(&mut certpool);
    Ok(())
}

fn send_new_entry_service_message(tx: ServiceSender, entry: &StorageEntry) {
    let bus_message = ServiceMessage::NewOperation(entry.entry_signed().hash().into());
    tx.send(bus_message)
        .expect("Expected to be able to send a ServiceMessage on ServiceSender");
}

async fn get_latest_seq(context: &Context, log_id: &LogId, author: &Author) -> Option<SeqNum> {
    context
        .0
        .store
        .get_latest_entry(author, log_id)
        .await
        .ok()
        .flatten()
        .map(|entry| *entry.entry_decoded().seq_num())
}
