// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, ensure, Result};
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::AsOperation;
use p2panda_rs::storage_provider::traits::StorageProvider;

/// Verify that a claimed seq num is the next sequence number following the latest.
///
/// Performs two steps:
/// - Established the expected sequence number either by incrementing the latest (when passed), or instantiating
///   it to sequence nomber 1 when None
/// - ensures the claimed sequence number is equal to expected one  
pub fn is_next_seq_num(latest_seq_num: Option<&SeqNum>, claimed_seq_num: &SeqNum) -> Result<()> {
    let expected_seq_num = match latest_seq_num {
        Some(seq_num) => {
            let mut seq_num = seq_num.to_owned();
            increment_seq_num(&mut seq_num)
        }
        None => Ok(SeqNum::default()),
    }?;

    ensure!(
        expected_seq_num == *claimed_seq_num,
        anyhow!(
            "Entry's claimed seq num of {} does not match expected seq num of {} for given author and log",
            claimed_seq_num.as_u64(),
            expected_seq_num.as_u64()
        )
    );
    Ok(())
}

/// Verify that an entry's claimed log id matches what we expect from the claimed document id.
///
/// This method handles both the case where the claimed log id already exists for this author
/// and where it is a new log.
///
/// The following steps are taken:
/// - Retrieve the stored log id for the claimed document id
///   - If found, ensure it matches the claimed log ig
///   - If not found retrieve the next available log id for this author and ensure that matches
pub async fn verify_log_id<S: StorageProvider>(
    store: &S,
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
            let expected_log_id = next_log_id(store, author).await?;

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

// Get the _expected_ skiplink for the passed entry.
//
// This method retrieves the expected skiplink given the author, log and seq num
// of an entry. It _does not_ verify that it matches the claimed skiplink
// encoded on the passed entry.
//
// If the expected skiplink could not be found in the database an error is returned.
pub async fn get_expected_skiplink<S: StorageProvider>(
    store: &S,
    author: &Author,
    log_id: &LogId,
    seq_num: &SeqNum,
) -> Result<S::StorageEntry> {
    ensure!(
        !seq_num.is_first(),
        anyhow!("Entry with seq num 1 can not have skiplink")
    );
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

/// Ensure that a document is not deleted.
///
/// Takes the following steps:
/// - Retrieve all operations for the given document id
/// - Ensure none of them contain a DELETE action
pub async fn ensure_document_not_deleted<S: StorageProvider>(
    store: &S,
    document_id: &DocumentId,
) -> Result<()> {
    // Retrieve the document view for this document, if none is found, then it is deleted.
    let operations = store.get_operations_by_document_id(document_id).await?;
    ensure!(
        !operations.iter().any(|operation| operation.is_delete()),
        anyhow!("Document is deleted")
    );
    Ok(())
}

/// Retrieve the next log id for a given author.
///
/// Takes the following steps:
/// - Retrieve the latest log id for the given author
/// - Safely increment it by 1
pub async fn next_log_id<S: StorageProvider>(store: &S, author: &Author) -> Result<LogId> {
    let latest_log_id = store.latest_log_id(author).await?;

    let next_log_id = match latest_log_id {
        Some(mut log_id) => log_id.next(),
        None => Some(LogId::default()),
    };

    match next_log_id {
        Some(log_id) => Ok(log_id),
        None => Err(anyhow!("Max log id reached")),
    }
}

/// Safely increment a sequence number by one.
pub fn increment_seq_num(seq_num: &mut SeqNum) -> Result<SeqNum> {
    match seq_num.next() {
        Some(next_seq_num) => Ok(next_seq_num),
        None => Err(anyhow!("Max sequence number reached")),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::test_utils::constants::PRIVATE_KEY;
    use p2panda_rs::test_utils::fixtures::random_document_id;
    use rstest::rstest;

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    use super::{
        ensure_document_not_deleted, get_expected_skiplink, is_next_seq_num, verify_log_id,
    };

    #[rstest]
    #[case::valid_seq_num(Some(SeqNum::new(2).unwrap()), SeqNum::new(3).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 2 does not match expected seq num of 3 for given author and log"
    )]
    #[case::seq_num_already_used(Some(SeqNum::new(2).unwrap()),SeqNum::new(2).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 4 does not match expected seq num of 3 for given author and log"
    )]
    #[case::seq_num_too_high(Some(SeqNum::new(2).unwrap()),SeqNum::new(4).unwrap())]
    #[should_panic(
        expected = "Entry's claimed seq num of 3 does not match expected seq num of 1 for given author and log"
    )]
    #[case::no_seq_num(None, SeqNum::new(3).unwrap())]
    fn verifies_seq_num(#[case] latest_seq_num: Option<SeqNum>, #[case] claimed_seq_num: SeqNum) {
        is_next_seq_num(latest_seq_num.as_ref(), &claimed_seq_num).unwrap();
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
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
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
    #[case::author_does_not_exist(KeyPair::new(), LogId::default(), SeqNum::new(5).unwrap())]
    #[should_panic(
        expected = "Expected skiplink for <Author 53fc96>, log id 4 and seq num 7 not found in database"
    )]
    #[case::log_id_is_wrong(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(4), SeqNum::new(7).unwrap())]
    #[should_panic(expected = "Entry with seq num 1 can not have skiplink")]
    #[case::seq_num_is_one(KeyPair::from_private_key_str(PRIVATE_KEY).unwrap(), LogId::new(0), SeqNum::new(1).unwrap())]
    fn gets_expected_skiplink(
        #[case] key_pair: KeyPair,
        #[case] log_id: LogId,
        #[case] seq_num: SeqNum,
        #[from(test_db)]
        #[with(7, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            get_expected_skiplink(&db.store, &author, &log_id, &seq_num)
                .await
                .unwrap();
        })
    }

    #[rstest]
    #[should_panic(expected = "Document is deleted")]
    fn identifies_deleted_document(
        #[from(test_db)]
        #[with(3, 1, 1, true)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let document_id = db.test_data.documents.first().unwrap();
            ensure_document_not_deleted(&db.store, document_id)
                .await
                .unwrap();
        })
    }

    #[rstest]
    fn identifies_not_deleted_document(
        #[from(test_db)]
        #[with(3, 1, 1, false)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase<SqlStorage>| async move {
            let document_id = db.test_data.documents.first().unwrap();
            ensure_document_not_deleted(&db.store, document_id)
                .await
                .unwrap();
        })
    }
}
