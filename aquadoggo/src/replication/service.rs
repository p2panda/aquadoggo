// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bamboo_rs_core_ed25519_yasmf::verify::verify_batch;
use log::{debug, trace, warn};
use p2panda_rs::api::publish;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::LogId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::storage_provider::traits::EntryStore;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::types::StorageEntry;
use crate::manager::{ServiceReadySender, Shutdown};

/// Replication service polling other nodes frequently to ask them about new entries from a defined
/// set of authors and log ids.
pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    _tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Prepare replication configuration
    let config = &context.config.replication;
    let connection_interval = Duration::from_secs(config.connection_interval_seconds);
    let _public_keys_to_replicate = Arc::new(config.public_keys_to_replicate.clone());
    let remote_peers = Arc::new(config.remote_peers.clone());

    // Start replication service
    let handle = task::spawn(async move {
        loop {
            if !remote_peers.is_empty() {
                debug!("Starting replication with remote peers");
            }

            // TODO: Replication goes here.....

            // Wait a couple of seconds before we attempt next replication requests
            tokio::time::sleep(connection_interval).await;
        }
    });

    debug!("Replication service is ready");
    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about replication service being ready");
    };

    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    Ok(())
}

/// Helper method to verify a batch of entries coming from an untrusted peer.
async fn _verify_entries(entries: &[StorageEntry], context: &Context) -> Result<()> {
    // Get the first entry (assumes they're sorted by seq_num smallest to largest)
    // @TODO: We can not trust that the other peer sorted the entries for us?
    let first_entry = entries.get(0).cloned();
    let mut entries_to_verify = entries.to_vec();

    match first_entry.as_ref() {
        // If the entry is the first in the log then we can don't need to attempt to get the
        // skiplink and previous
        Some(entry) if *entry.seq_num() == SeqNum::new(1).unwrap() => {
            trace!("First entry had seq_num 1 do no need to get previous entries in db");
        }
        Some(entry) => {
            trace!("Getting certificate pool for entries");
            _add_certpool_to_entries_for_verification(&mut entries_to_verify, entry, context)
                .await?;
        }
        None => (),
    }

    let entries_to_verify: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries_to_verify
        .iter()
        .map(|entry| {
            (
                entry.into_bytes(),
                entry.payload().map(|payload| payload.into_bytes()),
            )
        })
        .collect();

    // @TODO: Do we want to use our own verification here?
    verify_batch(&entries_to_verify)?;

    Ok(())
}

/// Helper method to insert a batch of verified entries into the database.
async fn _insert_new_entries(
    new_entries: &[StorageEntry],
    context: &Context,
    tx: ServiceSender,
) -> Result<()> {
    for entry in new_entries {
        // This is the method used to publish entries arriving from clients. They all contain a
        // payload (operation).
        //
        // @TODO: This is not a great fit for replication, as it performs validation we either do
        // not need or have already done in a previous step. We plan to refactor this into a more
        // modular set of methods which can definitely be used here more cleanly. For now, we do it
        // this way.

        let encoded_operation = entry
            .payload()
            .expect("All stored entries contain an operation");
        let operation = decode_operation(encoded_operation)?;

        let schema = context
            .schema_provider
            .get(operation.schema_id())
            .await
            .ok_or("Schema not supported")
            .map_err(|err| anyhow!(format!("{:?}", err)))?;

        publish(
            &context.0.store,
            &schema,
            &entry.encoded_entry,
            &operation,
            encoded_operation,
        )
        .await
        .map_err(|err| anyhow!(format!("Error inserting new entry into db: {:?}", err)))?;

        // Send new entry & operation to other services.
        _send_new_entry_service_message(tx.clone(), entry);
    }

    Ok(())
}

/// Helper method to retreive all entries from certificate pool to be able to verify Bamboo log
/// integrity.
async fn _add_certpool_to_entries_for_verification(
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
            first_entry.public_key(),
            first_entry.log_id(),
            first_entry.seq_num(),
        )
        .await?;

    trace!("Got {} certpool entries", certpool.len());
    entries.append(&mut certpool);

    Ok(())
}

/// Helper method to inform other services (like materialisation service) about new operations.
fn _send_new_entry_service_message(tx: ServiceSender, entry: &StorageEntry) {
    let bus_message = ServiceMessage::NewOperation(entry.hash().into());

    if tx.send(bus_message).is_err() {
        // Silently fail here as we don't mind if there are no subscribers
    }
}

/// Helper method to get the latest sequence number of a log and public_key.
async fn _get_latest_seq_num(
    context: &Context,
    log_id: &LogId,
    public_key: &PublicKey,
) -> Option<SeqNum> {
    context
        .store
        .get_latest_entry(public_key, log_id)
        .await
        .ok()
        .flatten()
        .map(|entry| *entry.seq_num())
}
