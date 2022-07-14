// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::{EntrySigned, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, AsVerifiedOperation, OperationAction, OperationEncoded, VerifiedOperation,
};
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, EntryStore, LogStore, OperationStore,
};
use p2panda_rs::Validate;

use crate::db::provider::SqlStorage;
use crate::db::stores::{StorageEntry, StorageLog};
use crate::graphql::client::NextEntryArguments;
use crate::validation::{
    ensure_document_not_deleted, ensure_entry_contains_expected_log_id,
    get_validate_document_id_for_view_id, validate_operation_against_schema,
};

pub async fn next_args(
    store: &SqlStorage,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
    // Validate the public key.
    public_key.validate()?;

    // If no document_view_id is passed then this is a request for publishing a CREATE operation
    // and we return the args for the next free log by this author.
    if document_view_id.is_none() {
        let log_id = store.next_log_id(&public_key).await?;
        return Ok(NextEntryArguments {
            backlink: None,
            skiplink: None,
            seq_num: SeqNum::default().into(),
            log_id: log_id.into(),
        });
    }

    // We can unwrap here as we know document_view_id is some.
    // Access and validate the document view id.
    //
    // We can unwrap here as we know document_view_id is some.
    let document_view_id = document_view_id.unwrap();

    // Get the document_id for this document_view_id. This performs several validation steps (check
    // method doc string).
    let document_id = get_validate_document_id_for_view_id(store, document_view_id).await?;

    // Check the document is not deleted.
    ensure_document_not_deleted(store, &document_id).await?;

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

pub async fn publish(
    store: &SqlStorage,
    entry_signed: &EntrySigned,
    operation_encoded: &OperationEncoded,
) -> Result<NextEntryArguments> {
    /////////////////////////////////////////////////////
    // VALIDATE ENTRY AND OPERATION INTERNAL INTEGRITY //
    /////////////////////////////////////////////////////

    let entry = StorageEntry::new(entry_signed, operation_encoded)?;
    let operation = VerifiedOperation::new_from_entry(entry_signed, operation_encoded)?;

    ///////////////////////////
    // VALIDATE ENTRY VALUES //
    ///////////////////////////

    // Gets the expected backlink of an entry from the store and validates that
    // it matches the claimed on.
    let entry_backlink_bytes = store
        .try_get_backlink(&entry)
        .await?
        .map(|link| link.entry_bytes());

    // Gets the expected skiplink of an entry from the store and validates that
    // it matches the claimed on.
    let entry_skiplink_bytes = store
        .try_get_skiplink(&entry)
        .await?
        .map(|link| link.entry_bytes());

    // Verify bamboo entry integrity, including encoding, signature of the entry correct back-
    // and skiplinks
    bamboo_rs_core_ed25519_yasmf::verify(
        &entry.entry_bytes(),
        Some(&operation_encoded.to_bytes()),
        entry_skiplink_bytes.as_deref(),
        entry_backlink_bytes.as_deref(),
    )?;

    // @TODO: Missing a step here where we check if the author has published to this node before, and also
    // if we know of any other nodes they have published to. Not sure how to do this yet.

    ///////////////////////////////
    // VALIDATE OPERATION VALUES //
    ///////////////////////////////

    validate_operation_against_schema(store, operation.operation()).await?;

    let document_id = match operation.action() {
        OperationAction::Create => {
            let next_log_id = store.next_log_id(&entry.author()).await?;
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &next_log_id).await?;

            // Derive the document id for this new document.
            entry.hash().into()
        }
        _ => {
            // We can unwrap previous operations here as we know all UPDATE and DELETE operations contain them.
            let previous_operations = operation.previous_operations().unwrap();
            // Get the document_id for the document_view_id contained in previous operations.
            // This performs several validation steps (check method doc string).
            let document_id =
                get_validate_document_id_for_view_id(&store, &previous_operations).await?;

            // Check the document is not deleted.
            ensure_document_not_deleted(&store, &document_id)
                .await
                .map_err(|_| {
                    "You are trying to update or delete a document which has been deleted"
                })?;

            // Check if there is a log_id registered for this document and public key already in the store.
            match store.get(&entry.author(), &document_id).await? {
                Some(log_id) => {
                    // If there is, check it matches the log id encoded in the entry.
                    ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &log_id).await?;
                }
                None => {
                    // If there isn't, check that the next log id for this author matches the one encoded in
                    // the entry.
                    let next_log_id = store.next_log_id(&entry.author()).await?;
                    ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &next_log_id)
                        .await?;
                }
            };
            document_id
        }
    };

    /////////////////////////////////////
    // DETERMINE NEXT ENTRY ARG VALUES //
    /////////////////////////////////////

    let next_seq_num = match entry.seq_num().next() {
        Some(seq_num) => Ok(seq_num),
        None => Err("Max sequence number reached for this log"),
    }?;

    let skiplink = store.determine_next_skiplink(&entry).await?;

    ///////////////////////////////////////////////////
    // INSERT LOG, ENTRY AND OPERATION INTO DATABASE //
    ///////////////////////////////////////////////////

    // If this is a CREATE operation it goes into a new log which we insert here.
    if operation.is_create() {
        let log = StorageLog::new(
            &entry.author(),
            &entry.operation().schema(),
            &document_id,
            &entry.log_id(),
        );

        store.insert_log(log).await?;
    }
    // Insert the entry into the store.
    store.insert_entry(entry.clone()).await?;
    // Insert the operation into the store.
    store.insert_operation(&operation, &document_id).await?;

    Ok(NextEntryArguments {
        log_id: entry.log_id().into(),
        seq_num: next_seq_num.into(),
        backlink: Some(entry.hash().into()),
        skiplink: skiplink.map(|hash| hash.into()),
    })
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::Author;
    use p2panda_rs::test_utils::fixtures::public_key;
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::client::NextEntryArguments;

    use super::next_args;

    #[rstest]
    fn gets_next_args(#[from(test_db)] runner: TestDatabaseRunner, public_key: Author) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let result = next_args(&db.store, &public_key, None).await;
            assert!(result.is_ok());

            let expected_next_args = NextEntryArguments {
                backlink: None,
                skiplink: None,
                log_id: LogId::default().into(),
                seq_num: SeqNum::default().into(),
            };

            assert_eq!(expected_next_args, result.unwrap())
        });
    }
}
