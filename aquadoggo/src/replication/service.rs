// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bamboo_rs_core_ed25519_yasmf::verify::verify_batch;
use log::{debug, error, trace, warn};
use p2panda_rs::entry::LogId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::AsVerifiedOperation;
use p2panda_rs::operation::VerifiedOperation;
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::stores::StorageEntry;
use crate::domain::publish;
use crate::graphql::replication::client;
use crate::manager::Shutdown;

/// Replication service polling other nodes frequently to ask them about new entries from a defined
/// set of authors and log ids.
pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
) -> Result<()> {
    // Prepare replication configuration
    let config = &context.config.replication;
    let connection_interval = Duration::from_secs(config.connection_interval_seconds);
    let authors_to_replicate = Arc::new(config.authors_to_replicate.clone());
    let remote_peers = Arc::new(config.remote_peers.clone());

    // Start replication service
    let handle = task::spawn(async move {
        loop {
            if !remote_peers.is_empty() {
                debug!("Starting replication with remote peers");
            }

            // Ask every remote peer about latest entries of log ids and authors
            for remote_peer in remote_peers.clone().iter() {
                for author_to_replicate in authors_to_replicate.clone().iter() {
                    let author = author_to_replicate.author().clone();
                    let log_ids = author_to_replicate.log_ids().clone();

                    for log_id in log_ids {
                        // Get the latest sequence number we have for this log and author
                        let latest_seq_num = get_latest_seq_num(&context, &log_id, &author).await;
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

            // Wait a couple of seconds before we attempt next replication requests
            tokio::time::sleep(connection_interval).await;
        }
    });

    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    Ok(())
}

/// Helper method to verify a batch of entries coming from an untrusted peer.
async fn verify_entries(entries: &[StorageEntry], context: &Context) -> Result<()> {
    // Get the first entry (assumes they're sorted by seq_num smallest to largest)
    // @TODO: We can not trust that the other peer sorted the entries for us?
    let first_entry = entries.get(0).cloned();
    let mut entries_to_verify = entries.to_vec();

    match first_entry.as_ref() {
        // If the entry is the first in the log then we can don't need to attempt to get the
        // skiplink and previous
        Some(entry) if entry.seq_num() == SeqNum::new(1).unwrap() => {
            trace!("First entry had seq_num 1 do no need to get previous entries in db");
        }
        Some(entry) => {
            trace!("Getting certificate pool for entries");
            add_certpool_to_entries_for_verification(&mut entries_to_verify, entry, context)
                .await?;
        }
        None => (),
    }

    let entries_to_verify: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries_to_verify
        .iter()
        .map(|entry| {
            (
                entry.entry_bytes(),
                entry
                    .operation_encoded()
                    .map(|operation| operation.to_bytes()),
            )
        })
        .collect();

    verify_batch(&entries_to_verify)?;

    Ok(())
}

/// Helper method to insert a batch of verified entries into the database.
async fn insert_new_entries(
    new_entries: &[StorageEntry],
    context: &Context,
    tx: ServiceSender,
) -> Result<()> {
    for entry in new_entries {
        // Here we the method used to publish entries arriving from clients. They all contain a
        // payload (operation).
        //
        // @TODO: This is not a great fit for replication, as it performs validation we either do
        // not need or already done in a previous step. We plan to refactor this into a more
        // modular set of methods which can definitely be used here more cleanly. For now, we do it
        // this way.
        //
        // @TODO: Additionally, when we implement payload deletion and partial replication we will
        // be expecting entries to arrive here possibly without payloads.

        publish(
            &context.0.store,
            entry.entry_signed(),
            entry
                .operation_encoded()
                .expect("All stored entries contain an operation"),
        )
        .await
        .map_err(|err| anyhow!(format!("Error inserting new entry into db: {:?}", err)))?;

        // Send new entry & operation to other services.
        send_new_entry_service_message(tx.clone(), entry);
    }

    Ok(())
}

/// Helper method to retreive all entries from certificate pool to be able to verify Bamboo log
/// integrity.
async fn add_certpool_to_entries_for_verification(
    entries: &mut Vec<StorageEntry>,
    first_entry: &StorageEntry,
    context: &Context,
) -> Result<()> {
    trace!("Getting certificate pool for entries");

    // @TODO: This gets the certificate pool from the database, but what if we need to get it from
    // the other peer?
    let mut certpool = context
        .0
        .store
        .get_certificate_pool(
            &first_entry.author(),
            &first_entry.log_id(),
            &first_entry.seq_num(),
        )
        .await?;

    trace!("Got {} certpool entries", certpool.len());
    entries.append(&mut certpool);

    Ok(())
}

/// Helper method to inform other services (like materialization service) about new operations.
fn send_new_entry_service_message(tx: ServiceSender, entry: &StorageEntry) {
    let bus_message = ServiceMessage::NewOperation(entry.entry_signed().hash().into());

    if tx.send(bus_message).is_err() {
        // Silently fail here as we don't mind if there are no subscribers
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

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};
    use std::time::Duration;

    use p2panda_rs::identity::Author;
    use p2panda_rs::storage_provider::traits::EntryStore;
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use rstest::rstest;
    use tokio::sync::broadcast;
    use tokio::task;

    use crate::context::Context;
    use crate::db::stores::test_utils::{
        populate_test_db, with_db_manager_teardown, PopulateDatabaseConfig, TestDatabaseManager,
    };
    use crate::http::http_service;
    use crate::replication::ReplicationConfiguration;
    use crate::test_helpers::shutdown_handle;
    use crate::Configuration;

    use super::replication_service;

    // @TODO: This will be replaced with using `ctor` in this PR:
    // https://github.com/p2panda/aquadoggo/pull/166
    fn init_env_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[rstest]
    fn full_replication() {
        with_db_manager_teardown(|db_manager: TestDatabaseManager| async move {
            init_env_logger();

            // Build and populate Billie's database
            let mut billie_db = db_manager.create("sqlite::memory:").await;
            let populate_db_config = PopulateDatabaseConfig {
                no_of_entries: 1,
                no_of_logs: 1,
                no_of_authors: 1,
                ..Default::default()
            };
            populate_test_db(&mut billie_db, &populate_db_config).await;

            // Launch HTTP service of Billie
            let (tx, _rx) = broadcast::channel(16);
            let tx_billie = tx.clone();
            let shutdown_billie = shutdown_handle();
            let context_billie = Context::new(
                billie_db.store.clone(),
                Configuration {
                    http_port: 3022,
                    ..Configuration::default()
                },
            );

            let http_server_billie = task::spawn(async {
                http_service(context_billie, shutdown_billie, tx_billie)
                    .await
                    .unwrap();
            });

            // Our test database helper already populated the database for us. We retreive the
            // public keys here of the authors who created these test data entries
            let public_key = billie_db
                .test_data
                .key_pairs
                .first()
                .unwrap()
                .public_key()
                .to_owned();

            let author = Author::try_from(public_key).unwrap();
            let log_ids: Vec<u64> = vec![0];
            let author_str: String = author.as_str().into();
            let endpoint: String = "http://localhost:3022/graphql".into();

            // Construct database and context for Ada
            let config_ada = Configuration {
                replication: ReplicationConfiguration {
                    authors_to_replicate: vec![(author_str, log_ids).try_into().unwrap()],
                    remote_peers: vec![endpoint],
                    ..ReplicationConfiguration::default()
                },
                ..Configuration::default()
            };
            let ada_db = db_manager.create("sqlite::memory:").await;
            let context_ada = Context::new(ada_db.store.clone(), config_ada);
            let tx_ada = tx.clone();
            let shutdown_ada = shutdown_handle();

            // Ada starts replication service to get data from Billies GraphQL API
            let replication_service_ada = task::spawn(async {
                replication_service(context_ada, shutdown_ada, tx_ada)
                    .await
                    .unwrap();
            });

            // Wait a little bit for replication to take place
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Make sure the services did not stop
            assert!(!http_server_billie.is_finished());
            assert!(!replication_service_ada.is_finished());

            // Check the entry arrived into Ada's database
            let entries = ada_db
                .store
                .get_entries_by_schema(&SCHEMA_ID.parse().unwrap())
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
        })
    }
}
