// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, ensure, Result};
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{Entry, EntrySigned, LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, LogStore};

use crate::db::provider::SqlStorage;
use crate::db::stores::StorageEntry;
use crate::db::traits::DocumentStore;

// @TODO: This method will be used in a follow-up PR
//
// /// Validate an operation against it's claimed schema.
// ///
// /// This performs two steps and will return an error if either fail:
// /// - try to retrieve the claimed schema from storage
// /// - if the schema is found, validate the operation against it
// pub async fn validate_operation_against_schema(
//     store: &SqlStorage,
//     operation: &Operation,
// ) -> Result<()> {
//     // Retrieve the schema for this operation from the store.
//     //
//     // @TODO Later we will want to use the schema provider for this, now we just get all schema and find the
//     // one we are interested in.
//     let all_schema = store.get_all_schema().await?;
//     let schema = all_schema
//         .iter()
//         .find(|schema| schema.id() == &operation.schema());
//
//     // If the schema we want doesn't exist, then error now.
//     ensure!(schema.is_some(), anyhow!("Schema not found"));
//     let schema = schema.unwrap();
//
//     // Validate that the operation correctly follows the stated schema.
//     validate_cbor(&schema.as_cddl(), &operation.to_cbor())?;
//
//     // All went well, return Ok.
//     Ok(())
// }

/// Compare the log id encoded on an entry with the expected log id.
///
/// This performs one validatin step:
/// - does the claimed log id match the expected one
pub fn ensure_log_ids_equal(claimed_log_id: &LogId, expected_log_id: &LogId) -> Result<()> {
    ensure!(
        claimed_log_id == expected_log_id,
        anyhow!(
            "Entry's claimed log id of {} does not match expected log id of {} for given author",
            claimed_log_id.as_u64(),
            expected_log_id.as_u64()
        )
    );
    Ok(())
}

pub async fn verify_seq_num(
    store: &SqlStorage,
    author: &Author,
    log_id: &LogId,
    claimed_seq_num: &SeqNum,
) -> Result<()> {
    // Retrieve the latest entry for the claimed author and log_id.
    let latest_entry = store.get_latest_entry(author, log_id).await?;

    match latest_entry {
        Some(latest_entry) => {
            // If one was found, increment it's seq_num to find the next expected.
            let expected_seq_num = latest_entry
                .seq_num()
                .next()
                .expect("Max seq number reached");

            // Ensure the next expected matches the claimed seq_num.
            ensure!(
                expected_seq_num == *claimed_seq_num,
                anyhow!(
                    "Entry's claimed seq num of {} does not match expected seq num of {} for given author and log",
                    claimed_seq_num.as_u64(),
                    expected_seq_num.as_u64()
                )
            );
        }
        None => {
            // If no entry was found, then this is the first entry in a new log and seq_num should be 1.
            ensure!(claimed_seq_num.is_first(), anyhow!(
                "Entry's claimed seq num of {} does not match expected seq num of 1 when creating a new log",
                claimed_seq_num.as_u64()
            ))
        }
    };
    Ok(())
}

/// Verify that an entry's claimed log id matches what we expect from the claimed document id.
///
/// This method handles both the case where the claimed log id already exists for this author
/// and where it is a new log. In both verify that:
/// - The claimed log id matches the expected one
pub async fn verify_log_id(
    store: &SqlStorage,
    author: &Author,
    claimed_log_id: &LogId,
    document_id: &DocumentId,
) -> Result<()> {
    // Check if there is a log_id registered for this document and public key already in the store.
    match store.get(author, document_id).await? {
        Some(expected_log_id) => {
            // If there is, check it matches the log id encoded in the entry.
            ensure!(
                *claimed_log_id == expected_log_id,
                anyhow!(
                    "Entry's claimed log id of {} does not match expected log id of {} for given author and document",
                    claimed_log_id.as_u64(),
                    expected_log_id.as_u64()
                )
            );
        }
        None => {
            // If there isn't, check that the next log id for this author matches the one encoded in
            // the entry.
            let expected_log_id = store.next_log_id(author).await?;
            ensure!(
                *claimed_log_id == expected_log_id,
                anyhow!(
                    "Entry's claimed log id of {} does not match expected next log id of {} for given author",
                    claimed_log_id.as_u64(),
                    expected_log_id.as_u64()
                )
            );
        }
    };
    Ok(())
}

// Get the _expected_ backlink for the passed entry.
//
// This method retrieves the expected backlink given the author, log and seq num
// of the passed entry. It _does not_ verify that it matches the claimed backlink
// encoded on the passed entry.
//
// If the expected backlink could not be found in the database an error is returned.
//
// Return value can be none when the seq num of the passed entry is 1.
//
// @TODO This depricates `try_get_backlink()` on storage provider.
pub async fn get_expected_backlink(
    store: &SqlStorage,
    author: &Author,
    log_id: &LogId,
    seq_num: &SeqNum,
) -> Result<Option<StorageEntry>> {
    if seq_num.is_first() {
        return Ok(None);
    };

    // Unwrap as we know this isn't the first sequence number because of the above condition
    let backlink_seq_num = SeqNum::new(seq_num.as_u64() - 1).unwrap();
    let expected_backlink = store
        .get_entry_at_seq_num(author, log_id, &backlink_seq_num)
        .await?;

    ensure!(
        expected_backlink.is_some(),
        anyhow!(
            "Expected backlink for {}, log id {} and seq num {} not found in database",
            author,
            log_id.as_u64(),
            seq_num.as_u64()
        )
    );

    Ok(expected_backlink)
}

// Get the _expected_ skiplink for the passed entry.
//
// This method retrieves the expected skiplink given the author, log and seq num
// of the passed entry. It _does not_ verify that it matches the claimed skiplink
// encoded on the passed entry.
//
// If the expected skiplink could not be found in the database an error is returned.
//
// Return value can be none when skiplink for this entry is not required or claimed.
//
// @TODO This depricates `try_get_skiplink()` on storage provider.
pub async fn get_expected_skiplink(
    store: &SqlStorage,
    author: &Author,
    log_id: &LogId,
    seq_num: &SeqNum,
) -> Result<StorageEntry> {
    // Derive the expected skiplink seq number from this entries claimed sequence number
    let expected_skiplink = match seq_num.skiplink_seq_num() {
        // Retrieve the expected skiplink from the database
        Some(seq_num) => {
            let expected_skiplink = store.get_entry_at_seq_num(author, log_id, &seq_num).await?;
            expected_skiplink
        }
        // Or if there is no skiplink for entries at this sequence number return None
        None => None,
    };

    ensure!(
        expected_skiplink.is_some(),
        anyhow!(
            "Expected skiplink for {}, log id {} and seq num {} not found in database",
            author,
            log_id.as_u64(),
            seq_num.as_u64()
        )
    );

    Ok(expected_skiplink.unwrap())
}

/// Verifies the encoded bamboo entry bytes and payload.
///
/// Internally this calls `bamboo_rs_core_ed25519_yasmf::verify` on the passed values.
pub fn verify_bamboo_entry(
    entry: &EntrySigned,
    operation: &OperationEncoded,
    backlink: Option<&EntrySigned>,
    skiplink: Option<&EntrySigned>,
) -> Result<()> {
    // Verify bamboo entry integrity, including encoding, signature of the entry correct back-
    // and skiplinks
    bamboo_rs_core_ed25519_yasmf::verify(
        &entry.to_bytes(),
        Some(&operation.to_bytes()),
        skiplink.map(|link| link.to_bytes()).as_deref(),
        backlink.map(|link| link.to_bytes()).as_deref(),
    )?;

    Ok(())
}

/// Ensure that a document is not deleted.
///
/// Verifies that:
/// - the document id we will be performing an UPDATE or DELETE on exists in the database
pub async fn ensure_document_not_deleted(
    store: &SqlStorage,
    document_id: &DocumentId,
) -> Result<()> {
    // @TODO: We can do this more clearly by checking for a deleted flag when we have handled
    // how to access that.

    // Retrieve the document view for this document, if none is found, then it is deleted.
    let document = store.get_document_by_id(document_id).await?;
    ensure!(document.is_some(), anyhow!("Document is deleted"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::{Entry, LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::test_utils::constants::PRIVATE_KEY;
    use p2panda_rs::test_utils::fixtures::random_document_id;
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    use super::{ensure_log_ids_equal, get_expected_skiplink, verify_log_id, verify_seq_num};

    #[rstest]
    #[case(LogId::new(0))]
    #[should_panic(
        expected = "Entry's claimed log id of 1 does not match expected log id of 0 for given author"
    )]
    #[case(LogId::new(1))]
    fn ensures_entry_contains_expected_log_id(#[case] claimed_log_id: LogId) {
        ensure_log_ids_equal(&claimed_log_id, &LogId::default()).unwrap();
    }

    #[rstest]
    #[case::valid_seq_num(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(), SeqNum::new(3).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 2 does not match expected seq num of 3 for given author and log"
    )]
    #[case::seq_num_already_used(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(),SeqNum::new(2).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 4 does not match expected seq num of 3 for given author and log"
    )]
    #[case::seq_num_too_high(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(),SeqNum::new(4).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 3 does not match expected seq num of 1 when creating a new log"
    )]
    #[case::author_wrong_so_new_log(KeyPair::new(), LogId::default(), SeqNum::new(3).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 3 does not match expected seq num of 1 when creating a new log"
    )]
    #[case::log_id_wrong_so_new_log(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(1), SeqNum::new(3).unwrap())]
    fn verifies_seq_num(
        #[case] key_pair: KeyPair,
        #[case] log_id: LogId,
        #[case] claimed_seq_num: SeqNum,
        #[from(test_db)]
        #[with(2, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            verify_seq_num(&db.store, &author, &log_id, &claimed_seq_num)
                .await
                .unwrap();
        })
    }

    #[rstest]
    #[case::existing_document(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(), None)]
    #[case::new_document(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(2), Some(random_document_id()))]
    #[case::existing_document_new_author(KeyPair::new(), LogId::new(0), None)]
    #[should_panic(
        expected = "Entry's claimed log id of 1 does not match expected log id of 0 for given author and document"
    )]
    #[case::already_occupied_log_id_for_existing_document(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(1), None)]
    #[should_panic(
        expected = "Entry's claimed log id of 2 does not match expected log id of 0 for given author and document"
    )]
    #[case::new_log_id_for_existing_document(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(2), None)]
    #[should_panic(
        expected = "Entry's claimed log id of 1 does not match expected next log id of 0 for given author"
    )]
    #[case::new_author_not_next_log_id(KeyPair::new(), LogId::new(1), None)]
    #[should_panic(
        expected = "Entry's claimed log id of 0 does not match expected next log id of 2 for given author"
    )]
    #[case::new_document_occupied_log_id(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(0), Some(random_document_id()))]
    #[should_panic(
        expected = "Entry's claimed log id of 3 does not match expected next log id of 2 for given author"
    )]
    #[case::new_document_not_next_log_id(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(3), Some(random_document_id()))]
    fn verifies_log_id(
        #[case] key_pair: KeyPair,
        #[case] claimed_log_id: LogId,
        #[case] document_id: Option<DocumentId>,
        #[from(test_db)]
        #[with(2, 2, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Unwrap the passed document id or select the first valid one from the database.
            let document_id =
                document_id.unwrap_or_else(|| db.test_data.documents.first().unwrap().to_owned());

            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            verify_log_id(&db.store, &author, &claimed_log_id, &document_id)
                .await
                .unwrap();
        })
    }

    #[rstest]
    #[case::expected_skiplink_is_in_store(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(), SeqNum::new(13).unwrap())]
    #[case::expected_skiplink_is_in_store_and_is_same_as_backlink(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(), SeqNum::new(4).unwrap())]
    #[should_panic(
        expected = "Expected skiplink for <Author 53fc96>, log id 0 and seq num 20 not found in database"
    )]
    #[case::skiplink_not_in_store(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::default(), SeqNum::new(20).unwrap())]
    #[should_panic]
    #[case::author_does_not_exist(KeyPair::new(), LogId::default(), SeqNum::new(20).unwrap())]
    #[should_panic(
        expected = "Expected skiplink for <Author 53fc96>, log id 4 and seq num 7 not found in database"
    )]
    #[case::log_id_is_wrong(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(4), SeqNum::new(7).unwrap())]
    fn gets_expected_skiplink(
        #[case] key_pair: KeyPair,
        #[case] log_id: LogId,
        #[case] seq_num: SeqNum,
        #[from(test_db)]
        #[with(7, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            get_expected_skiplink(&db.store, &author, &log_id, &seq_num)
                .await
                .unwrap();
        })
    }
}
