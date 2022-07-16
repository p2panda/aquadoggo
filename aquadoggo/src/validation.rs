// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, ensure, Result};
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{Entry, EntrySigned, LogId, SeqNum};
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
/// - does the log id of the passed entry match the expected one
pub fn ensure_entry_contains_expected_log_id(entry: &Entry, expected_log_id: &LogId) -> Result<()> {
    ensure!(
        expected_log_id == entry.log_id(),
        anyhow!(
            "Entries claimed log id of {} does not match expected log id of {} for given author",
            entry.log_id().as_u64(),
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
    entry: &StorageEntry,
    document_id: &DocumentId,
) -> Result<()> {
    // Check if there is a log_id registered for this document and public key already in the store.
    match store.get(&entry.author(), document_id).await? {
        Some(log_id) => {
            // If there is, check it matches the log id encoded in the entry.
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &log_id)?;
        }
        None => {
            // If there isn't, check that the next log id for this author matches the one encoded in
            // the entry.
            let next_log_id = store.next_log_id(&entry.author()).await?;
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &next_log_id)?;
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
    entry: &StorageEntry,
) -> Result<Option<StorageEntry>> {
    if entry.seq_num().is_first() {
        return Ok(None);
    };

    // Unwrap as we know this isn't the first sequence number because of the above condition
    let backlink_seq_num = SeqNum::new(entry.seq_num().as_u64() - 1).unwrap();
    let expected_backlink = store
        .get_entry_at_seq_num(&entry.author(), &entry.log_id(), &backlink_seq_num)
        .await?;

    ensure!(
        expected_backlink.is_some(),
        anyhow!(
            "Expected backlink for entry {} not found in database",
            entry.hash()
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
    entry: &StorageEntry,
) -> Result<Option<StorageEntry>> {
    // If a skiplink isn't required and it wasn't provided, return already now
    if !is_lipmaa_required(entry.seq_num().as_u64()) && entry.skiplink_hash().is_none() {
        return Ok(None);
    };

    // Derive the expected skiplink seq number from this entries claimed sequence number
    let expected_skiplink = match entry.seq_num().skiplink_seq_num() {
        // Retrieve the expected skiplink from the database
        Some(seq_num) => {
            let expected_skiplink = store
                .get_entry_at_seq_num(&entry.author(), &entry.log_id(), &seq_num)
                .await?;

            ensure!(
                expected_skiplink.is_some(),
                anyhow!(
                    "Expected skiplink for entry {} not found in database",
                    entry.hash()
                )
            );

            expected_skiplink
        }
        // Or if there is no skiplink for entries at this sequence number return None
        None => None,
    };

    Ok(expected_skiplink)
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

/// Verify that an entry's claimed log id matches what we expect from the claimed document id.
///
/// This method handles both the case where the claimed log id already exists for this author
/// and where it is a new log. In both verify that:
/// - The claimed log id matches the expected one
pub async fn verify_log_id(
    store: &SqlStorage,
    entry: &StorageEntry,
    document_id: &DocumentId,
) -> Result<()> {
    // Check if there is a log_id registered for this document and public key already in the store.
    match store.get(&entry.author(), document_id).await? {
        Some(log_id) => {
            // If there is, check it matches the log id encoded in the entry.
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &log_id)?;
        }
        None => {
            // If there isn't, check that the next log id for this author matches the one encoded in
            // the entry.
            let next_log_id = store.next_log_id(&entry.author()).await?;
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &next_log_id)?;
        }
    };
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
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{Entry, LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationFields, OperationId};
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{
        entry, operation, operation_fields, public_key, random_document_view_id,
    };
    use rstest::rstest;

    use crate::db::stores::test_utils::{send_to_store, test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::client::NextEntryArguments;

    use super::ensure_entry_contains_expected_log_id;

    #[rstest]
    #[case(LogId::new(0))]
    #[should_panic(
        expected = "Entries claimed log id of 0 does not match expected log id of 1 for given author"
    )]
    #[case(LogId::new(1))]
    fn ensures_entry_contains_expected_log_id(entry: Entry, #[case] expected_log_id: LogId) {
        ensure_entry_contains_expected_log_id(&entry, &expected_log_id).unwrap();
    }
}
