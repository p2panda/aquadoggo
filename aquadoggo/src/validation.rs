// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, ensure, Result};
use p2panda_rs::cddl::validate_cbor;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{Entry, LogId};
use p2panda_rs::operation::{AsOperation, Operation, OperationEncoded};
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, LogStore};

use crate::db::provider::SqlStorage;
use crate::db::stores::StorageEntry;
use crate::db::traits::{DocumentStore, SchemaStore};

/// Validate an operation against it's claimed schema.
///
/// This performs two steps and will return an error if either fail:
/// - try to retrieve the claimed schema from storage
/// - if the schema is found, validate the operation against it
pub async fn validate_operation_against_schema(
    store: &SqlStorage,
    operation: &Operation,
) -> Result<()> {
    // Retrieve the schema for this operation from the store.
    //
    // @TODO Later we will want to use the schema provider for this, now we just get all schema and find the
    // one we are interested in.
    let all_schema = store.get_all_schema().await?;
    let schema = all_schema
        .iter()
        .find(|schema| schema.id() == &operation.schema());

    // If the schema we want doesn't exist, then error now.
    ensure!(schema.is_some(), anyhow!("Schema not found"));
    let schema = schema.unwrap();

    // Validate that the operation correctly follows the stated schema.
    validate_cbor(&schema.as_cddl(), &operation.to_cbor())?;

    // All went well, return Ok.
    Ok(())
}

/// Compare the log id encoded on an entry with the expected log id.
///
/// This performs one validatin step:
/// - does the log id of the passed entry match the expected one
pub async fn ensure_entry_contains_expected_log_id(
    entry: &Entry,
    expected_log_id: &LogId,
) -> Result<()> {
    ensure!(
        expected_log_id == entry.log_id(),
        anyhow!("Entries claimed log id does not match expected")
    );
    Ok(())
}

/// Validate that the values encoded on an entry are the ones we expect.
///
/// This performs three validation steps:
/// - ensure the entry's claimed backlink exist in the database
/// - ensure the entry's claimed skiplink exist in the database
/// - verify the bamboo entry's integrity
pub async fn validate_entry(
    store: &SqlStorage,
    entry: &StorageEntry,
    operation: &OperationEncoded,
) -> Result<()> {
    // @TODO: We may be duplicating backlink and skiplink verification here

    // Gets the expected backlink of an entry from the store and validates that
    // it matches the claimed on.
    let backlink = store.try_get_backlink(&entry).await?;

    // Gets the expected skiplink of an entry from the store and validates that
    // it matches the claimed on.
    let skiplink = store.try_get_skiplink(&entry).await?;

    // Verify bamboo entry integrity, including encoding, signature of the entry correct back-
    // and skiplinks
    bamboo_rs_core_ed25519_yasmf::verify(
        &entry.entry_bytes(),
        Some(&operation.to_bytes()),
        skiplink.map(|link| link.entry_bytes()).as_deref(),
        backlink.map(|link| link.entry_bytes()).as_deref(),
    )?;

    Ok(())
}

/// Verify that an entry's claimed log id matches what we expect from the claimed document id.
///
/// This method handles both the case where the claimed log id already exists for this author
/// and where it is a new log. In both verify that:
/// - The claimed log id matches the expected one
pub async fn validate_stated_log_id(
    store: &SqlStorage,
    entry: &StorageEntry,
    document_id: &DocumentId,
) -> Result<()> {
    // Check if there is a log_id registered for this document and public key already in the store.
    match store.get(&entry.author(), document_id).await? {
        Some(log_id) => {
            // If there is, check it matches the log id encoded in the entry.
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &log_id).await?;
        }
        None => {
            // If there isn't, check that the next log id for this author matches the one encoded in
            // the entry.
            let next_log_id = store.next_log_id(&entry.author()).await?;
            ensure_entry_contains_expected_log_id(&entry.entry_decoded(), &next_log_id).await?;
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
