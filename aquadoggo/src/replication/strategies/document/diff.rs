// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use crate::db::types::StorageOperation;
use crate::db::SqlStore;
use crate::replication::TargetSet;

use log::trace;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
use p2panda_rs::Human;

/// Get the operations we have on the node for the passed document view id.
///
/// Note: If some of the operations identified in the document view id are not present on the node
/// we _still_ collect and return the ones we have. This occurs when the view id we're handling
/// was sent from the remote, and it contains branches we don't know about yet. We can still
/// diff the document only looking at the branches we _can_ identify though, so we still collect
/// and return the operations which are found.
async fn get_document_view_id_operations(
    store: &SqlStore,
    document_view_id: &DocumentViewId,
) -> Vec<StorageOperation> {
    let mut document_view_operations = Vec::new();
    for operation_id in document_view_id.iter() {
        if let Some(operation) = store
            .get_operation(operation_id)
            .await
            .expect("Fatal storage error")
        {
            document_view_operations.push(operation)
        }
        // We ignore None cases and continue the loop.
    }

    document_view_operations
}

/// Determine the document height for the passed documents identified by their document view id.
///
/// This retrieves the operations identified in the view id, iterates over them and returns the
/// highest `sorted_index`. In the case where the operation has not been reduced as part of a view
/// yet, it's `sorted_index` will be none. If this is the case for all operations in the view then
/// the document height will be `None`.
///
/// @TODO: This can be optimized with a single `get_document_height_by_view_id()` method on the store.
async fn determine_document_height(
    store: &SqlStore,
    document_view_id: &DocumentViewId,
) -> (Option<i32>, Vec<StorageOperation>) {
    let mut height = None::<i32>;

    let document_view_id_operations =
        get_document_view_id_operations(store, document_view_id).await;

    for operation in &document_view_id_operations {
        height = match operation.sorted_index {
            Some(index) => Some(height.map_or_else(
                || index,
                |current_index| {
                    if current_index < index {
                        index
                    } else {
                        current_index
                    }
                },
            )),
            None => height,
        };
    }

    (height, document_view_id_operations)
}

/// Compare a set of remote documents (identified by their document id and view id) with the
/// documents we have in our local store for the passed target set. Return a map of document ids
/// and heights signifying from where in the document the remote peer needs to be updated in order
/// to bring them in line with our current state.
pub async fn diff_documents(
    store: &SqlStore,
    target_set: TargetSet,
    remote_documents: Vec<(DocumentId, DocumentViewId)>,
) -> HashMap<DocumentId, i32> {
    let remote_documents: HashMap<DocumentId, DocumentViewId> =
        remote_documents.into_iter().collect();

    // Collect all documents which we have locally for the target set.
    let mut local_documents = HashMap::new();
    for schema_id in target_set.iter() {
        let schema_documents = store
            .get_documents_by_schema(schema_id)
            .await
            .expect("Fatal database error");
        local_documents.extend(
            schema_documents
                .into_iter()
                .map(|document| (document.id().to_owned(), document)),
        );
    }

    let mut remote_needs = HashMap::new();

    // Iterate over all local documents and perform a number of state checks in order to
    // efficiently conclude which operations we should send the remote.
    //
    // 1) If the remote doesn't know about our document then we already know they need all
    //    operations we have.
    // 2) If they know our document and the view ids are the same then we know the states are
    //    equal and we don't need to send anything.
    // 3) If they know our document but we can't calculate their height document height, then they
    //    are have operations to send us.
    // 4) If they know our document and we can calculate the height, then compare local and remote
    //    heights and send operations if they are behind our state.
    for (document_id, local_document) in local_documents {
        let remote_document_view_id = remote_documents.get(&document_id);

        if let Some(remote_document_view_id) = remote_document_view_id {
            // If the local and remote view ids are the same then we know they're at the same
            // state and we don't need to send anything.
            if local_document.view_id() == remote_document_view_id {
                trace!(
                    "Local and remote document views are equal {} <DocumentViewId {}>: no action required",
                    document_id.display(),
                    local_document.view_id().display()
                );
                // Continue to loop over the remaining documents.
                continue;
            }

            // Calculate the remote document height.
            let (remote_height, _) =
                determine_document_height(store, &remote_document_view_id).await;

            // If the height for the remote of this document couldn't be calculated then they are
            // further progressed for this document and so we don't need to send them anything.
            if remote_height.is_none() {
                trace!(
                    "Could not calculate remote document height {} <DocumentViewId {}>: no action required",
                    document_id.display(),
                    remote_document_view_id.display()
                );
                // Continue to loop over the remaining documents.
                continue;
            };

            // Safely unwrap as None case handled above.
            let remote_height = remote_height.unwrap();

            trace!(
                "Remote document height: {} <DocumentViewId {}> {remote_height}",
                local_document.id().display(),
                remote_document_view_id.display()
            );

            // Calculate the local document height.
            //
            // @TODO: This could be made more efficient if we store the current document height in the
            // store.
            let (local_height, _) =
                determine_document_height(store, local_document.view_id()).await;
            let local_height = local_height.expect("All local documents have been materialized");

            trace!(
                "Local document height: {} <DocumentViewId {}> {local_height:?}",
                local_document.id().display(),
                local_document.view_id().display()
            );

            // If the remote height is less than the local height then we want to send them
            // all operations at an index greater than the remote height.
            if remote_height < local_height {
                trace!("Local document height greater than remote: send new operations");
                remote_needs.insert(local_document.id().to_owned(), remote_height + 1);
            }
        } else {
            trace!(
                "Document not known by remote {}: send all document operations",
                document_id.display()
            );

            // The remote didn't know about this document yet so we send them everything we have.
            remote_needs.insert(local_document.id().to_owned(), 0_i32);
        };
    }

    remote_needs
}
