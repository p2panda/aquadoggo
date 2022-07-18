// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Result;
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::{EntrySigned, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, AsVerifiedOperation, OperationEncoded, VerifiedOperation,
};
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, EntryStore, LogStore, OperationStore,
};
use p2panda_rs::Validate;

use crate::db::provider::SqlStorage;
use crate::db::stores::{StorageEntry, StorageLog};
use crate::domain::{determine_document_id, get_validate_document_id_for_view_id};
use crate::graphql::client::NextEntryArguments;
use crate::validation::{
    get_expected_backlink, get_expected_skiplink, verify_bamboo_entry, verify_log_id,
    verify_seq_num,
};

// Helper method used in tests to retrieve next_args while skipping validation steps.
//
// The steps skipped are:
// -We do NOT check if a document has been deleted
pub async fn next_args_unverified(
    store: &SqlStorage,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
    // If no document_view_id is passed then this is a request for publishing a CREATE operation
    // and we return the args for the next free log by this author.
    if document_view_id.is_none() {
        let log_id = store.next_log_id(public_key).await?;
        return Ok(NextEntryArguments {
            backlink: None,
            skiplink: None,
            seq_num: SeqNum::default().into(),
            log_id: log_id.into(),
        });
    }

    // We can unwrap here as we know document_view_id is some.
    let document_view_id = document_view_id.unwrap();

    // Get the document_id for this document_view_id. This performs several validation steps (check
    // method doc string).
    let document_id = get_validate_document_id_for_view_id(store, document_view_id).await?;

    // Retrieve the log_id for the found document_id and author.
    //
    // (lolz, this method is just called `get()`)
    let log_id = match store.get(public_key, &document_id).await? {
        // This public key already wrote to this document, so we return the found log_id
        Some(log_id) => log_id,
        // This public_key never wrote to this document before so we return a new log_id
        None => store.next_log_id(public_key).await?,
    };

    // Get the latest entry in this log.
    let latest_entry = store.get_latest_entry(public_key, &log_id).await?;

    // Determine skiplink ("lipmaa"-link) entry in this log.
    //
    // If the latest entry is None, then the skiplink will also be None.
    let skiplink_hash = match latest_entry {
        Some(ref latest_entry) => store.determine_next_skiplink(latest_entry).await?,
        None => None,
    };

    // Determine the next sequence number by incrementing one from the latest entry seq num.
    //
    // If the latest entry is None, then we must be at seq num 1.
    let seq_num = match latest_entry {
        Some(ref latest_entry) => latest_entry
            .seq_num()
            .next()
            .expect("Max sequence number reached \\*o*/"),
        None => SeqNum::default(),
    };

    Ok(NextEntryArguments {
        backlink: latest_entry.map(|entry| entry.hash().into()),
        skiplink: skiplink_hash.map(|hash| hash.into()),
        seq_num: seq_num.into(),
        log_id: log_id.into(),
    })
}

/// A test method for publishing entries and operations without performing most validation
/// steps.
///
/// This is needed as in a testing environment we can't assume that all documents have been
/// materialised.
pub async fn publish_unverified(
    store: &SqlStorage,
    entry_signed: &EntrySigned,
    operation_encoded: &OperationEncoded,
) -> Result<NextEntryArguments> {
    // Internally this constructor performs several validation steps. Including checking the operation hash
    // matches the one encoded on the entry.
    let entry = StorageEntry::new(entry_signed, operation_encoded)?;
    let operation = VerifiedOperation::new_from_entry(entry_signed, operation_encoded)?;

    // Determine next entry args.
    let log_id = entry.log_id();
    let next_seq_num = match entry.seq_num().next() {
        Some(seq_num) => Ok(seq_num),
        None => Err("Max sequence number reached for this log"),
    }?;
    let backlink = Some(entry.hash());
    let skiplink = store.determine_next_skiplink(&entry).await?;

    // Determine the document id.
    let document_id = determine_document_id(store, &entry).await?;

    // If this is a CREATE operation it goes into a new log which we insert here.
    if operation.is_create() {
        let log = StorageLog::new(
            &entry.author(),
            &entry.operation().schema(),
            &document_id,
            &log_id,
        );

        store.insert_log(log).await?;
    }

    // Insert the entry into the store.
    store.insert_entry(entry.clone()).await?;
    // Insert the operation into the store.
    store.insert_operation(&operation, &document_id).await?;

    Ok(NextEntryArguments {
        log_id: log_id.into(),
        seq_num: next_seq_num.into(),
        backlink: backlink.map(|hash| hash.into()),
        skiplink: skiplink.map(|hash| hash.into()),
    })
}
