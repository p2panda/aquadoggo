// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashSet;

use anyhow::{anyhow, ensure, Result as AnyhowResult};
use async_graphql::Result;
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::{decode_entry, EntrySigned, LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, AsVerifiedOperation, Operation, OperationAction, OperationEncoded,
    VerifiedOperation,
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
    get_expected_skiplink, increment_seq_num, next_log_id, verify_bamboo_entry, verify_log_id,
    verify_seq_num,
};

/// Retrieve arguments required for constructing the next entry in a bamboo log for a specific
/// author and document.
///
/// We accept a `DocumentViewId` rather than a `DocumentId` as an argument and then identify
/// the document id based on operations already existing in the store. Doing this means a document
/// can be updated without knowing the document id itself.
///
/// This method is intended to be used behind a public API and so we assume all passed values
/// are in themselves valid.
///
/// The steps and validation checks this method performs are:
///
/// Check if a document view id was passed
/// - if it wasn't we are creating a new document, safely increment the latest log id for the passed author
///   and return args immediately
/// - if it was continue knowing we are updating an existing document
/// Determine the document id we are concerned with
/// - verify that all operations in the passed document view id exist in the database
/// - verify that all operations in the passed document view id are from the same document
/// - ensure the document is not deleted
/// Determine next arguments
/// - get the log id for this author and document id, or if none is found safely increment this authors
///   latest log id
/// - get the backlink entry (latest entry for this author and log)
/// - get the skiplink for this author, log and next seq num
/// - get the latest seq num for this author and log and safely increment
/// Return next arguments
pub async fn next_args(
    store: &SqlStorage,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
    // @TODO: We assume the passed Author and DocumentView are internally valid.

    // Init the next args with base default values.
    let mut next_args = NextEntryArguments {
        backlink: None,
        skiplink: None,
        seq_num: SeqNum::default().into(),
        log_id: LogId::default().into(),
    };

    ////////////////////////
    // HANDLE CREATE CASE //
    ////////////////////////

    // If no document_view_id is passed then this is a request for publishing a CREATE operation
    // and we return the args for the next free log by this author.
    if document_view_id.is_none() {
        let log_id = next_log_id(store, public_key).await?;
        next_args.log_id = log_id.into();
        return Ok(next_args);
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

    /////////////////////////
    // DETERMINE NEXT ARGS //
    /////////////////////////

    // Retrieve the log_id for the found document_id and author.
    //
    // (lolz, this method is just called `get()`)
    let log_id = store.get(public_key, &document_id).await?;

    // Check if an existing log id was found for this author and document.
    match log_id {
        // If it wasn't found, we just calculate the next log id safely and return the next args.
        None => {
            let next_log_id = next_log_id(store, public_key).await?;
            next_args.log_id = next_log_id.into()
        }
        // If one was found, we need to get the backlink and skiplink, and safely increment the seq num.
        Some(log_id) => {
            // Get the latest entry in this log.
            let latest_entry = store.get_latest_entry(public_key, &log_id).await?;

            // Determine the next sequence number by incrementing one from the latest entry seq num.
            //
            // If the latest entry is None, then we must be at seq num 1.
            let seq_num = match latest_entry {
                Some(ref latest_entry) => {
                    let mut latest_seq_num = latest_entry.seq_num();
                    increment_seq_num(&mut latest_seq_num)
                }
                None => Ok(SeqNum::default()),
            }?;

            // Check if skiplink is required and if it is get the entry and return it's hash
            let skiplink = if is_lipmaa_required(seq_num.as_u64()) {
                // Determine skiplink ("lipmaa"-link) entry in this log.
                let skiplink_seq_num = seq_num.skiplink_seq_num().unwrap();
                match store
                    .get_entry_at_seq_num(public_key, &log_id, &skiplink_seq_num)
                    .await?
                {
                    Some(entry) => Ok(Some(entry)),
                    None => Err(anyhow!("Expected next skiplink missing")),
                }
            } else {
                Ok(None)
            }?
            .map(|entry| entry.hash());

            next_args.backlink = latest_entry.map(|entry| entry.hash().into());
            next_args.skiplink = skiplink.map(|hash| hash.into());
            next_args.seq_num = seq_num.into();
            next_args.log_id = log_id.into();
        }
    };

    Ok(next_args)
}

/// Persist an entry and operation to storage after performing validation of claimed values against
/// expected values retrieved from storage.
///
/// Returns the arguments required for constructing the next entry in a bamboo log for the specified
/// author and document.
///
/// This method is intended to be used behind a public API and so we assume all passed values
/// are in themselves valid.
///
/// The steps and validation checks this method performs are:
///
/// Validate the values encoded on entry against what we expect based on our existing stored entries:
/// - verify the claimed sequence number against the expected next sequence number for the author and log
/// - get the expected backlink from storage
/// - get the expected skiplink from storage
/// - verify the bamboo entry (requires the expected backlink and skiplink to do this)
/// Ensure single node per author
/// - @TODO
/// Validate operation against it's claimed schema
/// - @TODO
/// Determine document id
/// - if this is a create operation
///   - derive the document id from the entry hash
/// - in all other cases
///   - verify that all operations in previous_operations exist in the database
///   - verify that all operations in previous_operations are from the same document
///   - ensure the document is not deleted
/// - verify the claimed log id matches the expected log id for this author and log
/// Determine the next arguments
/// Persist data
/// - if this is a new document
///   - store the new log
/// - store the entry
/// - store the opertion
/// Return next entry arguments
pub async fn publish(
    store: &SqlStorage,
    entry_encoded: &EntrySigned,
    operation_encoded: &OperationEncoded,
) -> Result<NextEntryArguments> {
    ////////////////////////////////
    // DECODE ENTRY AND OPERATION //
    ////////////////////////////////

    let entry = decode_entry(entry_encoded, Some(operation_encoded))?;
    let operation = Operation::from(operation_encoded);
    let author = entry_encoded.author();
    let log_id = entry.log_id();
    let seq_num = entry.seq_num();

    ///////////////////////////
    // VALIDATE ENTRY VALUES //
    ///////////////////////////

    // Verify the claimed seq num matches the expected seq num for this author and log.
    verify_seq_num(store, &author, log_id, seq_num).await?;

    // If a backlink is claimed, get the expected backlink from the database, errors if it can't be found.
    let backlink = match entry.backlink_hash() {
        Some(_) => Some(get_expected_backlink(store, &author, log_id, seq_num).await?),
        None => None,
    };

    // If a skiplink is claimed, get the expected skiplink from the database, errors
    // if it can't be found.
    let skiplink = match entry.skiplink_hash() {
        Some(_) => Some(get_expected_skiplink(store, &author, log_id, seq_num).await?),
        None => None,
    };

    // Verify the bamboo entry providing the encoded operation and retrieved backlink and skiplink.
    verify_bamboo_entry(
        entry_encoded,
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

    let document_id = match operation.action() {
        OperationAction::Create => {
            // Derive the document id for this new document.
            entry_encoded.hash().into()
        }
        _ => {
            // We can unwrap previous operations here as we know all UPDATE and DELETE operations contain them.
            let previous_operations = operation.previous_operations().unwrap();

            // Get the document_id for the document_view_id contained in previous operations.
            // This performs several validation steps (check method doc string).
            let document_id =
                get_validate_document_id_for_view_id(store, &previous_operations).await?;

            // Ensure the document isn't deleted.
            ensure_document_not_deleted(store, &document_id)
                .await
                .map_err(|_| {
                    "You are trying to update or delete a document which has been deleted"
                })?;

            document_id
        }
    };

    // Verify the claimed log id against the expected one for this document id and author.
    verify_log_id(store, &author, log_id, &document_id).await?;

    ///////////////
    // STORE LOG //
    ///////////////

    // If this is a CREATE operation it goes into a new log which we insert here.
    if operation.is_create() {
        let log = StorageLog::new(&author, &operation.schema(), &document_id, log_id);

        store.insert_log(log).await?;
    }

    ///////////////////////////////
    // STORE ENTRY AND OPERATION //
    ///////////////////////////////

    // Insert the entry into the store.
    store
        .insert_entry(StorageEntry::new(entry_encoded, operation_encoded)?)
        .await?;
    // Insert the operation into the store.
    store
        .insert_operation(
            &VerifiedOperation::new(&author, &entry_encoded.hash().into(), &operation).unwrap(),
            &document_id,
        )
        .await?;
    
    /////////////////////////////////////
    // DETERMINE NEXT ENTRY ARG VALUES //
    /////////////////////////////////////

    let next_seq_num = increment_seq_num(&mut seq_num.clone())?;
    let backlink = Some(entry_encoded.hash());
    
    // Check if skiplink is required and return hash if so
    let skiplink = if is_lipmaa_required(next_seq_num.as_u64()) {
            let skiplink_seq_num = next_seq_num.skiplink_seq_num().unwrap();
            match store
            .get_entry_at_seq_num(&author, log_id, &skiplink_seq_num)
            .await?
        {
            Some(entry) => Ok(Some(entry)),
            None => Err(anyhow!("Expected next skiplink missing")),
        }
    } else {
        Ok(None)
    }?
    .map(|entry| entry.hash());
    
    Ok(NextEntryArguments {
        log_id: log_id.clone().into(),
        seq_num: next_seq_num.into(),
        backlink: backlink.map(|hash| hash.into()),
        skiplink: skiplink.map(|hash| hash.into()),
    })
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
