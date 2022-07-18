// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashSet;

use anyhow::{anyhow, ensure, Result as AnyhowResult};
use async_graphql::Result;
use p2panda_rs::document::{DocumentId, DocumentViewId};
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
    ensure_document_not_deleted, ensure_log_ids_equal, get_expected_backlink,
    get_expected_skiplink, verify_bamboo_entry, verify_log_id, verify_seq_num,
};

pub async fn next_args(
    store: &SqlStorage,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
    // @TODO: We assume the passed Author and DocumentView are internally valid.

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

    // Check the document is not deleted.
    ensure_document_not_deleted(store, &document_id).await?;

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

pub async fn publish(
    store: &SqlStorage,
    entry_signed: &EntrySigned,
    operation_encoded: &OperationEncoded,
) -> Result<NextEntryArguments> {
    /////////////////////////////////////////////////////
    // VALIDATE ENTRY AND OPERATION INTERNAL INTEGRITY //
    /////////////////////////////////////////////////////

    // Internally this constructor performs several validation steps. Including checking the operation hash
    // matches the one encoded on the entry.
    //
    // @TODO: We still need a review of this section of the validation logic (internal data integrity) to
    // make sure we are doing everything we need and that there is not too much duplication (definitely doing
    // some ATM).
    let entry = StorageEntry::new(entry_signed, operation_encoded)?;
    let operation = VerifiedOperation::new_from_entry(entry_signed, operation_encoded)?;

    ///////////////////////////
    // VALIDATE ENTRY VALUES //
    ///////////////////////////

    // Verify the claimed seq num matches the expected seq num for this author and log.
    verify_seq_num(store, &entry.author(), &entry.log_id(), &entry.seq_num()).await?;

    // If a backlink is claimed, get the expected backlink from the database, errors if it can't be found.
    let backlink = match entry.backlink_hash() {
        Some(_) => Some(
            get_expected_backlink(store, &entry.author(), &entry.log_id(), &entry.seq_num())
                .await?,
        ),
        None => None,
    };

    // If a skiplink is claimed, get the expected skiplink from the database, errors
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

    ///////////////////////////////////
    // ENSURE SINGLE NODE PER AUTHOR //
    ///////////////////////////////////

    // @TODO: Missing a step here where we check if the author has published to this node before, and also
    // if we know of any other nodes they have published to. Not sure how to do this yet.

    ///////////////////////////////
    // VALIDATE OPERATION VALUES //
    ///////////////////////////////

    // @TODO: We skip this for now and will implement it in a follow-up PR
    // validate_operation_against_schema(store, operation.operation()).await?;

    //////////////////////////
    // DETERINE DOCUMENT ID //
    //////////////////////////

    let document_id = determine_document_id(store, &entry).await?;

    if !entry.operation().is_create() {
        // If this is an UPDATE or DELETE operation check the document is not deleted.
        ensure_document_not_deleted(store, &document_id)
            .await
            .map_err(|_| "You are trying to update or delete a document which has been deleted")?;
    }

    // Verify the claimed log id against the expected one for this document id and author.
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

pub async fn determine_document_id(store: &SqlStorage, entry: &StorageEntry) -> Result<DocumentId> {
    let document_id = match entry.operation().action() {
        OperationAction::Create => {
            let next_log_id = store.next_log_id(&entry.author()).await?;
            ensure_log_ids_equal(&entry.log_id(), &next_log_id)?;

            // Derive the document id for this new document.
            entry.hash().into()
        }
        _ => {
            // We can unwrap previous operations here as we know all UPDATE and DELETE operations contain them.
            let previous_operations = entry.operation().previous_operations().unwrap();

            // Get the document_id for the document_view_id contained in previous operations.
            // This performs several validation steps (check method doc string).
            let document_id =
                get_validate_document_id_for_view_id(store, &previous_operations).await?;

            document_id
        }
    };

    Ok(document_id)
}

/// Attempt to identify the document id for view id contained in a `next_args` request. This will fail if:
/// - any of the operations contained in the view id _don't_ exist in the store
/// - any of the operations contained in the view id return a different document id than any of the others
pub async fn get_validate_document_id_for_view_id(
    store: &SqlStorage,
    view_id: &DocumentViewId,
) -> AnyhowResult<DocumentId> {
    // If  a view id was passed, we want to check the following:
    // - Are all operations identified by this part of the same document?
    let mut found_document_ids: HashSet<DocumentId> = HashSet::new();
    for operation in view_id.clone().into_iter() {
        // If any operation can't be found return an error at this point already.
        let document_id = store.get_document_by_operation_id(&operation).await?;

        ensure!(
            document_id.is_some(),
            anyhow!("{} not found, could not determine document id", operation)
        );

        found_document_ids.insert(document_id.unwrap());
    }

    // We can unwrap here as there must be at least one document view else the error above would
    // have been triggered.
    let document_id = found_document_ids.iter().next().unwrap();

    ensure!(
        !found_document_ids.is_empty(),
        anyhow!("Invalid document view id: operartions in passed document view id originate from different documents")
    );
    Ok(document_id.to_owned())
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationFields, OperationId};
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{
        operation, operation_fields, public_key, random_document_view_id,
    };
    use rstest::rstest;

    use crate::db::stores::test_utils::{send_to_store, test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::client::NextEntryArguments;

    use super::{get_validate_document_id_for_view_id, next_args};

    #[rstest]
    fn gets_validates_document_id_for_view_id(
        #[from(test_db)] runner: TestDatabaseRunner,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let result = get_validate_document_id_for_view_id(&db.store, &document_view_id).await;
            assert!(result.is_err());
        });
    }

    #[rstest]
    fn invalid_document_view_id_missing_operations(
        #[from(test_db)] runner: TestDatabaseRunner,
        operation: Operation,
        operation_fields: OperationFields,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Store one entry and operation in the store.
            let (entry, _) = send_to_store(&db.store, &operation, None, &KeyPair::new()).await;
            let operation_one_id: OperationId = entry.hash().into();

            // Store another entry and operation which perform an update on the earlier operation.
            let update_operation = Operation::new_update(
                SCHEMA_ID.parse().unwrap(),
                operation_one_id.clone().into(),
                operation_fields,
            )
            .unwrap();

            let (entry, _) = send_to_store(
                &db.store,
                &update_operation,
                Some(&entry.hash().into()),
                &KeyPair::new(),
            )
            .await;
            let operation_two_id: OperationId = entry.hash().into();

            let result = get_validate_document_id_for_view_id(
                &db.store,
                &DocumentViewId::new(&[operation_one_id, operation_two_id]).unwrap(),
            )
            .await;
            assert!(result.is_ok());
        });
    }

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
