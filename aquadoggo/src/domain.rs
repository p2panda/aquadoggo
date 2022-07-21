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
};
use p2panda_rs::storage_provider::traits::{AsStorageEntry, AsStorageLog, StorageProvider};

use crate::graphql::client::NextEntryArguments;
use crate::validation::{
    ensure_document_not_deleted, get_expected_skiplink, increment_seq_num, is_next_seq_num,
    next_log_id, verify_log_id,
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
pub async fn next_args<S: StorageProvider>(
    store: &S,
    public_key: &Author,
    document_view_id: Option<&DocumentViewId>,
) -> Result<NextEntryArguments> {
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
                Some(get_expected_skiplink(store, public_key, &log_id, &seq_num).await?)
            } else {
                None
            }
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
pub async fn publish<S: StorageProvider>(
    store: &S,
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
    let latest_entry = store.get_latest_entry(&author, log_id).await?;
    let latest_seq_num = latest_entry.as_ref().map(|entry| entry.seq_num());
    is_next_seq_num(latest_seq_num.as_ref(), seq_num)?;

    // The backlink for this entry.
    let backlink = latest_entry;

    // If a skiplink is claimed, get the expected skiplink from the database, errors
    // if it can't be found.
    let skiplink = match entry.skiplink_hash() {
        Some(_) => Some(get_expected_skiplink(store, &author, log_id, seq_num).await?),
        None => None,
    };

    // Verify the bamboo entry providing the encoded operation and retrieved backlink and skiplink.
    bamboo_rs_core_ed25519_yasmf::verify(
        &entry_encoded.to_bytes(),
        Some(&operation_encoded.to_bytes()),
        skiplink.map(|entry| entry.entry_bytes()).as_deref(),
        backlink.map(|entry| entry.entry_bytes()).as_deref(),
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
        let log = S::StorageLog::new(&author, &operation.schema(), &document_id, log_id);

        store.insert_log(log).await?;
    }

    ///////////////////////////////
    // STORE ENTRY AND OPERATION //
    ///////////////////////////////

    // Insert the entry into the store.
    store
        .insert_entry(S::StorageEntry::new(entry_encoded, operation_encoded)?)
        .await?;
    // Insert the operation into the store.
    store
        .insert_operation(
            &S::StorageOperation::new(&author, &entry_encoded.hash().into(), &operation).unwrap(),
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
        log_id: (*log_id).into(),
        seq_num: next_seq_num.into(),
        backlink: backlink.map(|hash| hash.into()),
        skiplink: skiplink.map(|hash| hash.into()),
    })
}

/// Attempt to identify the document id for view id contained in a `next_args` request. This will fail if:
/// - any of the operations contained in the view id _don't_ exist in the store
/// - any of the operations contained in the view id return a different document id than any of the others
pub async fn get_validate_document_id_for_view_id<S: StorageProvider>(
    store: &S,
    view_id: &DocumentViewId,
) -> AnyhowResult<DocumentId> {
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
    let mut found_document_ids_iter = found_document_ids.iter();
    let document_id = found_document_ids_iter.next().unwrap();

    ensure!(
        found_document_ids_iter.next().is_none(),
        anyhow!("Invalid document view id: operations in passed document view id originate from different documents")
    );
    Ok(document_id.to_owned())
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationFields, OperationId};
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
    use p2panda_rs::test_utils::constants::{PRIVATE_KEY, SCHEMA_ID};
    use p2panda_rs::test_utils::db::MemoryStore;
    use p2panda_rs::test_utils::fixtures::{
        operation, operation_fields, public_key, random_document_view_id,
    };
    use rstest::rstest;

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{
        doggo_test_fields, encode_entry_and_operation, populate_test_db, send_to_store, test_db,
        test_db_config, PopulateDatabaseConfig, TestData, TestDatabase, TestDatabaseRunner,
    };
    use crate::domain::publish;
    use crate::graphql::client::NextEntryArguments;

    use super::{get_validate_document_id_for_view_id, next_args};

    type LogIdAndSeqNum = (u64, u64);

    /// Helper method for removing entries from a MemoryStore by Author & LogIdAndSeqNum.
    fn remove_entries(store: &MemoryStore, author: &Author, entries_to_remove: &[LogIdAndSeqNum]) {
        store.entries.lock().unwrap().retain(|_, entry| {
            !entries_to_remove.contains(&(entry.log_id().as_u64(), entry.seq_num().as_u64()))
                && &entry.author() == author
        });
    }

    /// Helper method for removing operations from a MemoryStore by Author & LogIdAndSeqNum.
    fn remove_operations(
        store: &MemoryStore,
        author: &Author,
        operations_to_remove: &[LogIdAndSeqNum],
    ) {
        for (hash, entry) in store.entries.lock().unwrap().iter() {
            if operations_to_remove.contains(&(entry.log_id().as_u64(), entry.seq_num().as_u64()))
                && &entry.author() == author
            {
                store
                    .operations
                    .lock()
                    .unwrap()
                    .remove(&hash.clone().into());
            }
        }
    }

    #[rstest]
    fn errors_when_passed_non_existent_view_id(
        #[from(test_db)] runner: TestDatabaseRunner,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
    ) {
        runner.with_db_teardown(|db: TestDatabase<SqlStorage>| async move {
            let result = get_validate_document_id_for_view_id(&db.store, &document_view_id).await;
            assert!(result.is_err());
        });
    }

    #[rstest]
    fn gets_document_id_for_view(
        #[from(test_db)] runner: TestDatabaseRunner,
        operation: Operation,
        operation_fields: OperationFields,
    ) {
        runner.with_db_teardown(|db: TestDatabase<SqlStorage>| async move {
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
    #[case::ok(&[(0, 8)], (0, 8))]
    #[should_panic(
        expected = "Expected skiplink for <Author 53fc96>, log id 0 and seq num 8 not found in database"
    )]
    #[case::skiplink_missing(&[(0, 4), (0, 8)], (0, 8))]
    #[should_panic(
        expected = "Entry's claimed seq num of 8 does not match expected seq num of 7 for given author and log"
    )]
    #[case::backlink_missing(&[(0, 7), (0, 8)], (0, 8))]
    #[should_panic(
        expected = "Entry's claimed seq num of 8 does not match expected seq num of 7 for given author and log"
    )]
    #[case::backlink_and_skiplink_missing(&[(0, 4), (0, 7), (0, 8)], (0, 8))]
    #[should_panic(
        expected = "Entry's claimed seq num of 8 does not match expected seq num of 9 for given author and log"
    )]
    #[case::seq_num_occupied_again(&[], (0, 8))]
    #[should_panic(
        expected = "Entry's claimed seq num of 7 does not match expected seq num of 9 for given author and log"
    )]
    #[case::seq_num_occupied_(&[], (0, 7))]
    #[should_panic(
        expected = "Entry's claimed seq num of 8 does not match expected seq num of 1 for given author and log"
    )]
    #[case::no_entries_yet(&[(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6), (0, 7), (0, 8)], (0, 8))]
    #[tokio::test]
    async fn publish_with_missing_entries(
        #[case] entries_to_remove: &[LogIdAndSeqNum],
        #[case] entry_to_publish: LogIdAndSeqNum,
        #[from(test_db_config)]
        #[with(8, 1, 1)]
        config: PopulateDatabaseConfig,
    ) {
        // Populate the db with 8 entries.
        let mut db = TestDatabase {
            store: MemoryStore::default(),
            test_data: TestData::default(),
        };
        populate_test_db(&mut db, &config).await;

        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();

        let next_entry = db
            .store
            .get_entry_at_seq_num(
                &author,
                &LogId::new(entry_to_publish.0),
                &SeqNum::new(entry_to_publish.1).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();

        remove_operations(&db.store, &author, entries_to_remove);
        remove_entries(&db.store, &author, entries_to_remove);

        let result = publish(
            &db.store,
            &next_entry.entry_signed(),
            &next_entry.operation_encoded().unwrap(),
        )
        .await;

        result.unwrap();
    }

    #[rstest]
    #[case::ok_single_writer(&[], &[(0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    // Weird case where all previous operations are on the same branch, but still valid.
    #[case::ok_many_previous_operations(&[], &[(0, 8), (0, 7), (0, 6)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[case::ok_multi_writer(&[], &[(0, 8)], KeyPair::new())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::previous_operation_missing(&[(0, 8)], &[(0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 51fbba> not found, could not determine document id")]
    #[case::one_of_some_previous_operations_missing(&[(0, 7)], &[(0, 7), (0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::one_of_some_previous_operations_missing(&[(0, 8)], &[(0, 7), (0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::missing_previous_operation_multi_writer(&[(0, 8)], &[(0, 8)], KeyPair::new())]
    #[should_panic(
        expected = "Invalid document view id: operations in passed document view id originate from different documents"
    )]
    #[case::previous_operations_invalid_multiple_document_id(&[], &[(0, 8), (1, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[tokio::test]
    async fn publish_with_missing_operations(
        // The operations to be removed from the        assert_eq!(result.map_err(|err|))
        #[case] operations_to_remove: &[LogIdAndSeqNum],
        // The previous operations described by their log id and seq number (log_id, seq_num)
        #[case] previous_operations: &[LogIdAndSeqNum],
        #[case] key_pair: KeyPair,
        #[from(test_db_config)]
        #[with(8, 2, 1)]
        config: PopulateDatabaseConfig,
    ) {
        // Populate the db with 8 entries.
        let mut db = TestDatabase {
            store: MemoryStore::default(),
            test_data: TestData::default(),
        };
        populate_test_db(&mut db, &config).await;
        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();

        // Get the document id.
        let document_id = db.test_data.documents.first().unwrap();

        // Map the passed &[LogIdAndSeqNum] into a DocumentViewId containing the claimed operations.
        let previous_operations: Vec<OperationId> = previous_operations
            .iter()
            .filter_map(|(log_id, seq_num)| {
                db.store
                    .entries
                    .lock()
                    .unwrap()
                    .values()
                    .find(|entry| {
                        entry.seq_num().as_u64() == *seq_num && entry.log_id.as_u64() == *log_id
                    })
                    .map(|entry| entry.hash().into())
            })
            .collect();
        // Construct document view id for previous operations.
        let document_view_id = DocumentViewId::new(&previous_operations).unwrap();

        // Compose the next operation.
        let next_operation = Operation::new_update(
            SCHEMA_ID.parse().unwrap(),
            document_view_id,
            operation_fields(doggo_test_fields()),
        )
        .unwrap();

        // Encode an entry and the operation.
        let (entry, operation) =
            encode_entry_and_operation(&db.store, &next_operation, &key_pair, Some(document_id))
                .await;

        remove_operations(&db.store, &author, operations_to_remove);

        // Publish the entry and operation.
        let result = publish(&db.store, &entry, &operation).await;

        result.unwrap();
    }

    #[rstest]
    #[case::ok_single_writer(&[], &[(0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    // Weird case where all previous operations are on the same branch, but still valid.
    #[case::ok_many_previous_operations(&[], &[(0, 8), (0, 7), (0, 6)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[case::ok_not_the_most_recent_document_view_id(&[], &[(0, 1)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[case::ok_multi_writer(&[], &[(0, 8)], KeyPair::new())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::previous_operation_missing(&[(0, 8)], &[(0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 51fbba> not found, could not determine document id")]
    #[case::one_of_some_previous_operations_missing(&[(0, 7)], &[(0, 7), (0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::one_of_some_previous_operations_missing(&[(0, 8)], &[(0, 7), (0, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[should_panic(expected = "<Operation 76e89a> not found, could not determine document id")]
    #[case::missing_previous_operation_multi_writer(&[(0, 8)], &[(0, 8)], KeyPair::new())]
    #[should_panic(
        expected = "Invalid document view id: operations in passed document view id originate from different documents"
    )]
    #[case::previous_operations_invalid_multiple_document_id(&[], &[(0, 8), (1, 8)], KeyPair::from_private_key_str(PRIVATE_KEY).unwrap())]
    #[tokio::test]
    async fn next_args_with_missing_operations(
        // The operations to be removed from the
        #[case] operations_to_remove: &[LogIdAndSeqNum],
        // The previous operations described by their log id and seq number (log_id, seq_num)
        #[case] document_view_id: &[LogIdAndSeqNum],
        #[case] key_pair: KeyPair,
        #[from(test_db_config)]
        #[with(8, 2, 1)]
        config: PopulateDatabaseConfig,
    ) {
        // Populate the db with 8 entries.
        let mut db = TestDatabase {
            store: MemoryStore::default(),
            test_data: TestData::default(),
        };
        populate_test_db(&mut db, &config).await;
        let author_with_removed_operations =
            Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();
        let author_making_request = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        // Map the passed &[LogIdAndSeqNum] into a DocumentViewId containing the claimed operations.
        let document_view_id: Vec<OperationId> = document_view_id
            .iter()
            .filter_map(|(log_id, seq_num)| {
                db.store
                    .entries
                    .lock()
                    .unwrap()
                    .values()
                    .find(|entry| {
                        entry.seq_num().as_u64() == *seq_num && entry.log_id.as_u64() == *log_id
                    })
                    .map(|entry| entry.hash().into())
            })
            .collect();

        // Construct document view id for previous operations.
        let document_view_id = DocumentViewId::new(&document_view_id).unwrap();

        remove_operations(
            &db.store,
            &author_with_removed_operations,
            operations_to_remove,
        );

        let result = next_args(&db.store, &author_making_request, Some(&document_view_id)).await;

        result.unwrap();
    }

    type SeqNumU64 = u64;
    type Backlink = Option<u64>;
    type Skiplink = Option<u64>;

    #[rstest]
    #[case(0, None, (1, None, None))]
    #[case(1, Some(1), (2, Some(1), None))]
    #[case(2, Some(2), (3, Some(2), None))]
    #[case(3, Some(3), (4, Some(3), Some(1)))]
    #[case(4, Some(4), (5, Some(4), None))]
    #[case(5, Some(5), (6, Some(5), None))]
    #[case(6, Some(6), (7, Some(6), None))]
    #[case(7, Some(7), (8, Some(7), Some(4)))]
    #[case(2, Some(1), (3, Some(2), None))]
    #[case(3, Some(1), (4, Some(3), Some(1)))]
    #[case(4, Some(1), (5, Some(4), None))]
    #[case(5, Some(1), (6, Some(5), None))]
    #[case(6, Some(1), (7, Some(6), None))]
    #[case(7, Some(1), (8, Some(7), Some(4)))]
    #[tokio::test]
    async fn next_args_with_expected_results(
        #[case] no_of_entries: usize,
        #[case] document_view_id: Option<SeqNumU64>,
        #[case] expected_next_args: (SeqNumU64, Backlink, Skiplink),
    ) {
        let config = PopulateDatabaseConfig {
            no_of_entries,
            no_of_logs: 1,
            no_of_authors: 1,
            ..PopulateDatabaseConfig::default()
        };

        let mut db = TestDatabase {
            store: MemoryStore::default(),
            test_data: TestData::default(),
        };
        populate_test_db(&mut db, &config).await;
        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();

        let document_view_id: Option<DocumentViewId> = document_view_id.map(|seq_num| {
            db.store
                .entries
                .lock()
                .unwrap()
                .values()
                .find(|entry| entry.seq_num().as_u64() == seq_num)
                .map(|entry| DocumentViewId::new(&[entry.hash().into()]).unwrap())
                .unwrap()
        });

        let expected_seq_num = SeqNum::new(expected_next_args.0).unwrap();
        let expected_log_id = LogId::default();
        let expected_backlink = match expected_next_args.1 {
            Some(backlink) => db
                .store
                .get_entry_at_seq_num(&author, &expected_log_id, &SeqNum::new(backlink).unwrap())
                .await
                .unwrap()
                .map(|entry| entry.hash()),
            None => None,
        };
        let expected_skiplink = match expected_next_args.2 {
            Some(skiplink) => db
                .store
                .get_entry_at_seq_num(&author, &expected_log_id, &SeqNum::new(skiplink).unwrap())
                .await
                .unwrap()
                .map(|entry| entry.hash()),
            None => None,
        };

        let expected_next_args = NextEntryArguments {
            log_id: expected_log_id.into(),
            seq_num: expected_seq_num.into(),
            backlink: expected_backlink.map(|hash| hash.into()),
            skiplink: expected_skiplink.map(|hash| hash.into()),
        };

        let result = next_args(&db.store, &author, document_view_id.as_ref()).await;
        assert_eq!(result.unwrap(), expected_next_args);
    }

    #[rstest]
    #[tokio::test]
    async fn gets_next_args_other_cases(
        public_key: Author,
        #[from(test_db_config)]
        #[with(7, 1, 1)]
        config: PopulateDatabaseConfig,
    ) {
        let mut db = TestDatabase {
            store: MemoryStore::default(),
            test_data: TestData::default(),
        };
        populate_test_db(&mut db, &config).await;

        // Get with no DocumentViewId given.
        let result = next_args(&db.store, &public_key, None).await;
        assert!(result.is_ok());
        assert_eq!(
            NextEntryArguments {
                backlink: None,
                skiplink: None,
                log_id: LogId::new(1).into(),
                seq_num: SeqNum::default().into(),
            },
            result.unwrap()
        );

        // Get with non-existent DocumentViewId given.
        let result = next_args(&db.store, &public_key, Some(&random_document_view_id())).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .as_str()
                .contains("could not determine document id") // This is a partial string match, preceded by "<Operation xxxxx> not found,"
        );

        // Here we are missing the skiplink.
        remove_entries(&db.store, &public_key, &[(0, 4)]);
        let document_id = db.test_data.documents.get(0).unwrap();
        let document_view_id =
            DocumentViewId::new(&[document_id.as_str().parse().unwrap()]).unwrap();

        let result = next_args(&db.store, &public_key, Some(&document_view_id)).await;
        assert_eq!(
            result.unwrap_err().message.as_str(),
            "Expected skiplink for <Author 53fc96>, log id 0 and seq num 8 not found in database"
        );
    }
}
