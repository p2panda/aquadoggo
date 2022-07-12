// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashSet;

use anyhow::{anyhow, ensure, Result};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;

/// Attempt to identify the document id for view id contained in a `next_args` request. This will fail if:
/// - any of the operations contained in the view id _don't_ exist in the store
/// - any of the operations contained in the view id return a different document id than any of the others
/// - the document was deleted
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

    // Retrieve the document view for this document, if none is found, then it is deleted.
    let document = store.get_document_by_id(&document_id).await?;
    ensure!(document.is_some(), anyhow!("Document is deleted"));

    Ok(document_id.to_owned())
}
