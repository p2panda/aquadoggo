use std::sync::Arc;

use log::{debug, warn, error};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::bus::ServiceMessage;
use crate::context::Context;
use crate::graphql::replication::client;
use crate::replication::service::{insert_new_entries, verify_entries};

pub async fn replicate_authors(
    authors_to_replicate: Arc<Vec<super::config::AuthorToReplicate>>,
    context: &Context,
    remote_peer: &String,
    tx: &tokio::sync::broadcast::Sender<ServiceMessage>,
) {
    for author_to_replicate in authors_to_replicate.clone().iter() {
        let author = author_to_replicate.author().clone();
        let log_ids = author_to_replicate.log_ids().clone();

        for log_id in log_ids {
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
                Ok(entries) => {
                    debug!(
                        "Received {} new entries from peer {}",
                        entries.len(),
                        remote_peer
                    );

                    if let Err(err) = verify_entries(&entries, &context).await {
                        warn!("Couldn't verify entries: {}", err);
                        continue;
                    }

                    insert_new_entries(&entries, &context, tx.clone())
                        .await
                        .unwrap_or_else(|err| error!("{:?}", err));
                }
                Err(err) => {
                    warn!(
                        "Replication request to peer {} failed: {}",
                        remote_peer, err
                    );
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
