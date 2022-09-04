// SPDX-License-Identifier: AGPL-3.0-or-later

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_graphql::connection::PageInfo;
use log::{debug, error, warn};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::bus::ServiceMessage;
use crate::context::Context;
use crate::graphql::pagination::PaginatedInfo;
use crate::graphql::replication::client;
use crate::replication::service::{insert_new_entries, verify_entries};

/// Fully replicate a set of schemas from a local peer.
pub async fn replicate_schemas(
    schemas_to_replicate: Arc<Vec<super::config::SchemaToReplicate>>,
    context: &Context,
    remote_peer: &String,
    tx: &tokio::sync::broadcast::Sender<ServiceMessage>,
) {
    let mut has_next_page=true;

    for schema_to_replicate in schemas_to_replicate.clone().iter() {
        let schema_id = schema_to_replicate.schema_id();

        while has_next_page {
            let result = client::logs_by_schema(remote_peer, schema_id).await;
            for edge in result.edges {
                let author = edge.node.author().clone();

                let log_ids = edge.node.log_ids().clone();
                replicate_author(author, log_ids, context, remote_peer, tx).await;
            }
        }
    }
}

pub async fn replicate_author(
    author: Author,
    log_ids: Vec<LogId>,
    context: &Context,
    remote_peer: &String,
    tx: &tokio::sync::broadcast::Sender<ServiceMessage>,
) {
    for log_id in log_ids {
        loop {
            // Get the latest sequence number we have for this log and author
            let latest_seq_num = get_latest_seq_num(context, &log_id, &author).await;
            debug!(
                "Latest entry sequence number of {} and {}: {:?}",
                log_id.as_u64(),
                author,
                latest_seq_num
            );

            // Make our replication request to the remote peer
            let response = client::entries_newer_than_seq_num(
                remote_peer,
                &log_id,
                &author,
                latest_seq_num.as_ref(),
            )
            .await;

            match response {
                Ok((info, entries)) => {
                    debug!(
                        "Received {} new entries from peer {}",
                        entries.len(),
                        remote_peer
                    );

                    if let Err(err) = verify_entries(&entries, context).await {
                        warn!("Couldn't verify entries: {}", err);
                        break;
                    }

                    insert_new_entries(&entries, context, tx.clone())
                        .await
                        .unwrap_or_else(|err| error!("{:?}", err));

                    if !info.has_next_page {
                        break;
                    }

                    // Wait a couple of seconds before we attempt next replication requests
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(err) => {
                    warn!(
                        "Replication request to peer {} failed: {}",
                        remote_peer, err
                    );
                    break;
                }
            }
        }
    }
}

/// Helper method to get the latest sequence number of a log and author.
async fn get_latest_seq_num(context: &Context, log_id: &LogId, author: &Author) -> Option<SeqNum> {
    context
        .store
        .get_latest_entry(author, log_id)
        .await
        .ok()
        .flatten()
        .map(|entry| *entry.entry_decoded().seq_num())
}
