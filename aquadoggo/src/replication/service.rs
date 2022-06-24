// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::VecDeque;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use bamboo_rs_core_ed25519_yasmf::verify::verify_batch;
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

                            // TODO: just for debug
                            let latest_seq = None;

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
                                let mut entries = VecDeque::from(entries);
                                debug!("Received {} new entries", entries.len());

                                // Get the first entry (assumes they're sorted by seq_num smallest
                                // to largest)
                                let first_entry = entries.get(0).map(|entry| entry.clone());

                                match first_entry.as_ref() {
                                    // If the entry is the first in the log then we can don't need
                                    // to attempt to get the skiplink and previous
                                    Some(entry) if entry.seq_num() == SeqNum::new(1).unwrap() => {
                                        trace!("first entry had seq_num 1 do no need to get previous entries in db");
                                    }
                                    Some(entry) => {
                                        match (entry.skiplink_hash(), entry.backlink_hash()) {
                                            (Some(skiplink_hash), Some(backlink_hash)) => {
                                                let skiplink = context
                                                    .0
                                                    .store
                                                    .get_entry_by_hash(&skiplink_hash)
                                                    .await
                                                    .expect("expected to get skiplink from db");

                                                let backlink = context
                                                    .0
                                                    .store
                                                    .get_entry_by_hash(&backlink_hash)
                                                    .await
                                                    .expect("expected to get backlink from db");

                                                if skiplink.is_none() || backlink.is_none() {
                                                    warn!("replication error. We received entries but didn't have the required backlink / skiplinks in the db");
                                                    continue;
                                                }

                                                entries.push_front(skiplink.unwrap());
                                                entries.push_front(backlink.unwrap());
                                            }
                                            (None, Some(backlink_hash))
                                                if !is_lipmaa_required(
                                                    entry.seq_num().as_u64(),
                                                ) =>
                                            {
                                                let backlink = context
                                                    .0
                                                    .store
                                                    .get_entry_by_hash(&backlink_hash)
                                                    .await
                                                    .expect("expected to get backlink from db");

                                                if backlink.is_none() {
                                                    warn!("replication error. We received entries but didn't have the required backlink in the db");
                                                    continue;
                                                }

                                                entries.push_front(backlink.unwrap());
                                            }
                                            (_, _) => {
                                                warn!("entry was an invalid format, not adding to our db: {:?}", entry);
                                            }
                                        }
                                    }
                                    None => {
                                        continue;
                                    }
                                }

                                let entries_to_verify = entries
                                    .iter()
                                    .map(|entry| (entry.entry_bytes(), Option::<Vec<u8>>::None))
                                    .collect::<Vec<_>>();

                                let verification_result = verify_batch(&entries_to_verify);

                                if verification_result.is_err() {
                                    warn!("couldn't verify entries: {:?}", verification_result);
                                    continue;
                                }

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
