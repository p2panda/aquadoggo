// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use crate::db::types::StorageOperation;
use crate::db::SqlStore;

use log::trace;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::OperationStore;
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

/// Compare a set of local documents and a set of remote documents (identified by their document
/// id and view id) and return a map of document ids and heights signifying from where the remote
/// peer needs to be updated in order to bring them in line with our current state.
pub async fn diff_documents(
    store: &SqlStore,
    local_documents: Vec<impl AsDocument>,
    remote_documents: Vec<(DocumentId, DocumentViewId)>,
) -> HashMap<DocumentId, i32> {
    // Calculate the document height for all passed remote documents.
    //
    // This is done by retrieving the operations for the remote document view id and taking the
    // hightest index. We also validate that the retrieved and claimed document id match the view
    // id (when found).
    let mut remote_document_heights = HashMap::new();

    for (document_id, document_view_id) in remote_documents {
        let (height, document_view_id_operations) =
            determine_document_height(store, &document_view_id).await;

        remote_document_heights.insert(document_id.clone(), (document_view_id, height));

        for operation in &document_view_id_operations {
            if operation.document_id != document_id {
                panic!("They tricked us!!")
            }
        }
    }

    let mut remote_needs = HashMap::new();

    // Iterate over all local documents.
    for local_document in local_documents {
        // Calculate the height for each local document.
        //
        // @TODO: This could be made more efficient if we store the current document height in the
        // store.
        let (local_height, _) = determine_document_height(store, local_document.view_id()).await;
        let local_height = local_height.expect("All local documents have been materialized");

        trace!(
            "Local document height: {} {} {local_height:?}",
            local_document.id().display(),
            local_document.view_id().display()
        );

        // Get details for the matching remote document by it's id.
        if let Some((remote_view_id, remote_height)) =
            remote_document_heights.get(local_document.id())
        {
            trace!(
                "Remote document height: {} {} {remote_height:?}",
                local_document.id().display(),
                remote_view_id.display()
            );

            // If the local and remote view ids are the same then we know they're at the same
            // state and don't need to send anything.
            //
            // @TODO: We can handle this case in the logic above so as to not perform unnecessary
            // document height calculations.
            if local_document.view_id() == remote_view_id {
                trace!("Local and remote document state matches (view ids are equal): no action required");
                continue;
            }

            // If the height for the remote of this document couldn't be calculated then they are
            // more progressed than us and so we should do nothing.
            if remote_height.is_none() {
                trace!("Remote document height greater than local: no action required");
                continue;
            };

            // Safely unwrap as None case handled above.
            let remote_height = remote_height.unwrap();

            // If the remote height is less than the local height then we want to send them
            // all operations at an index greater than the remote height.
            if remote_height < local_height {
                trace!("Local document height greater than remote: send new operations");
                remote_needs.insert(local_document.id().to_owned(), remote_height + 1);
            }
        } else {
            trace!("Document not known by remote: send all document operations");

            // The remote didn't know about this document yet so we send them everything we have.
            remote_needs.insert(local_document.id().to_owned(), 0_i32);
        };
    }

    remote_needs
}
