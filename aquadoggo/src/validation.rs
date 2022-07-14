// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashSet;

use anyhow::{anyhow, ensure, Result};
use p2panda_rs::cddl::validate_cbor;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::{Entry, LogId};
use p2panda_rs::operation::{AsOperation, Operation};
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::db::provider::SqlStorage;
use crate::db::traits::{DocumentStore, SchemaStore};

/// Attempt to identify the document id for view id contained in a `next_args` request. This will fail if:
/// - any of the operations contained in the view id _don't_ exist in the store
/// - any of the operations contained in the view id return a different document id than any of the others
pub async fn get_validate_document_id_for_view_id(
    store: &SqlStorage,
    view_id: &DocumentViewId,
) -> Result<DocumentId> {
    // If  a view id was passed, we want to check the following:
    // - Are all operations identified by this part of the same document?
    let mut found_document_ids: HashSet<DocumentId> = HashSet::new();
    for operation in view_id.clone().into_iter() {
        // If any operation can't be found return an error at this point already.
        let document_id = store.get_document_by_operation_id(&operation).await?;

        ensure!(
            document_id.is_some(),
            anyhow!("Document no found: operation in passed view id missing")
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

pub async fn ensure_document_not_deleted(
    store: &SqlStorage,
    document_id: &DocumentId,
) -> Result<()> {
    // Retrieve the document view for this document, if none is found, then it is deleted.
    let document = store.get_document_by_id(&document_id).await?;
    ensure!(document.is_some(), anyhow!("Document is deleted"));
    Ok(())
}
