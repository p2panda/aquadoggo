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

// Helper method used in tests to retrieve next_args while skipping some validation steps.
//
// This is needed as in a testing setup we can't assume that all documents will be materialised.
//
// The steps skipped are:
// -We do NOT check if a document has been deleted
pub async fn next_args_without_strict_validation(
    store: &SqlStorage,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
    //////////////////////////
    // VALIDATE PASSED ARGS //
    //////////////////////////

    // Validate the public key.
    public_key.validate()?;

    // Validate the document id if passed.
    match document_view_id {
        Some(id) => id.validate(),
        None => Ok(()),
    }?;

    ////////////////////////
    // HANDLE CREATE CASE //
    ////////////////////////

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

    ///////////////////////////
    // DETERMINE DOCUMENT ID //
    ///////////////////////////

    // We can unwrap here as we know document_view_id is some.
    let document_view_id = document_view_id.unwrap();

    // Get the document_id for this document_view_id. This performs several validation steps (check
    // method doc string).
    let document_id = get_validate_document_id_for_view_id(store, document_view_id).await?;

    // Here we DO NOT check if the document is deleted as in a testing environment we can't assume all documents
    // have been materialised.

    ////////////////////////////////
    // DETERMINE NEXT ARGS LOG ID //
    ////////////////////////////////

    // Retrieve the log_id for the found document_id and author.
    //
    // (lolz, this method is just called `get()`)
    let log_id = match store.get(public_key, &document_id).await? {
        // This public key already wrote to this document, so we return the found log_id
        Some(log_id) => log_id,
        // This public_key never wrote to this document before so we return a new log_id
        None => store.next_log_id(public_key).await?,
    };

    //////////////////////////////////
    // DETERMINE NEXT ARGS BACKLINK //
    //////////////////////////////////

    // Get the latest entry in this log.
    let latest_entry = store.get_latest_entry(public_key, &log_id).await?;

    //////////////////////////////////
    // DETERMINE NEXT ARGS SKIPLINK //
    //////////////////////////////////

    // Determine skiplink ("lipmaa"-link) entry in this log.
    //
    // If the latest entry is None, then the skiplink will also be None.
    let skiplink_hash = match latest_entry {
        Some(ref latest_entry) => store.determine_next_skiplink(latest_entry).await?,
        None => None,
    };

    //////////////////////////////////
    // DETERMINE NEXT ARGS SEQ NUM ///
    //////////////////////////////////

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

/// A test method for publishing entries and operations without performing some validation
/// steps.
///
/// This is needed as in a testing environment we can't assume that all documents have been
/// materialised.
///
/// The skipped steps are:
/// - we do NOT validate the operation against it's schema
/// - we do NOT check if a document is deleted
pub async fn publish_without_strict_validation(
    store: &SqlStorage,
    entry_signed: &EntrySigned,
    operation_encoded: &OperationEncoded,
) -> Result<NextEntryArguments> {
    /////////////////////////////////////////////////////
    // VALIDATE ENTRY AND OPERATION INTERNAL INTEGRITY //
    /////////////////////////////////////////////////////

    // Internally this constructor performs several validation steps. Including checking the operation hash
    // matches the one encoded on the entry.
    let entry = StorageEntry::new(entry_signed, operation_encoded)?;
    let operation = VerifiedOperation::new_from_entry(entry_signed, operation_encoded)?;

    ///////////////////////////
    // VALIDATE ENTRY VALUES //
    ///////////////////////////

    // Verify the claimed seq num matches the expected seq num for this author and log.
    verify_seq_num(store, &entry.author(), &entry.log_id(), &entry.seq_num()).await?;

    // Get the expected backlink for this entry, errors if it can't be found.
    let backlink =
        get_expected_backlink(store, &entry.author(), &entry.log_id(), &entry.seq_num()).await?;

    // If a skiplink hash was provided get the expected skiplink from the database, errors
    // if it can't be found.
    let skiplink = match entry.skiplink_hash() {
        Some(_) => Some(
            get_expected_skiplink(store, &entry.author(), &entry.log_id(), &entry.seq_num())
                .await?,
        ),
        None => None,
    };

    // Verify the bamboo entry providing the encoded operation and retrieved backlink and skiplink.
    verify_bamboo_entry(
        entry_signed,
        operation_encoded,
        backlink
            .map(|entry| entry.entry_signed().to_owned())
            .as_ref(),
        skiplink
            .map(|entry| entry.entry_signed().to_owned())
            .as_ref(),
    )?;

    //////////////////////////
    // DETERINE DOCUMENT ID //
    //////////////////////////

    let document_id = determine_document_id(store, &entry).await?;

    // Here we _don't_ check if the document is deleted as we can't assume in a testing environment
    // that all documents will be materialised.

    verify_log_id(store, &entry.author(), &entry.log_id(), &document_id).await?;

    /////////////////////////////////////
    // DETERMINE NEXT ENTRY ARG VALUES //
    /////////////////////////////////////

    let log_id = entry.log_id();
    let next_seq_num = match entry.seq_num().next() {
        Some(seq_num) => Ok(seq_num),
        None => Err("Max sequence number reached for this log"),
    }?;
    let backlink = Some(entry.hash());
    let skiplink = store.determine_next_skiplink(&entry).await?;

    ///////////////
    // STORE LOG //
    ///////////////

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

    ///////////////////////////////
    // STORE ENTRY AND OPERATION //
    ///////////////////////////////

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
