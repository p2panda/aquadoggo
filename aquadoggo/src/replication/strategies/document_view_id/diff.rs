// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use crate::db::types::StorageOperation;
use crate::db::SqlStore;

use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::OperationStore;

async fn get_document_view_id_operations(
    store: &SqlStore,
    document_view_id: &DocumentViewId,
) -> (Vec<StorageOperation>, bool) {
    let mut is_complete = true;
    let mut document_view_operations = Vec::new();
    for operation_id in document_view_id.iter() {
        let operation = store
            .get_operation(operation_id)
            .await
            .expect("Fatal storage error");
        if let Some(operation) = operation {
            document_view_operations.push(operation)
        } else {
            is_complete = false
        }
    }

    (document_view_operations, is_complete)
}

fn determine_document_height(document_view_operations: Vec<StorageOperation>) -> Option<i32> {
    let mut height = None::<i32>;

    for operation in document_view_operations {
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

    height
}

async fn diff_document_view_ids(
    store: &SqlStore,
    local_documents: Vec<impl AsDocument>,
    remote_document_view_ids: Vec<DocumentViewId>,
) -> HashMap<DocumentId, i32> {
    // Calculate the document id and height of all passed remote documents, identified by their document
    // view ids. Additionally we check if we have all operations for the document view id on
    // the node locally which is required for understanding if the height we are comparing is
    // for the same view id.
    //
    // We can observe documents in three different states:
    //
    // 1) We don't have any operations for this document view id and so can't calculate a document
    //   id or height
    // 2) We have some operations for this document view id and so can calculate the document id
    //   and current local height, but we know some operations are missing
    // 3) We have all operations for this document so can calculate the document id and local
    //   height and we know we have all operations for this document state
    let mut remote_document_heights = HashMap::new();

    for document_view_id in remote_document_view_ids {
        let mut document_id = None::<DocumentId>;
        let (document_view_id_operations, is_complete) =
            get_document_view_id_operations(store, &document_view_id).await;

        if let Some(operation) = document_view_id_operations.first() {
            document_id = Some(operation.clone().document_id);
        }

        let height = determine_document_height(document_view_id_operations);

        if let Some(document_id) = document_id {
            remote_document_heights.insert(document_id, (document_view_id, height, is_complete));
        }
    }

    // Calculate the document height for all passed local documents.
    let mut local_document_heights = HashMap::new();

    for document in &local_documents {
        let (document_view_id_operations, _) =
            get_document_view_id_operations(store, &document.view_id()).await;

        let height = determine_document_height(document_view_id_operations)
            .expect("All local documents have been materialized");

        local_document_heights.insert(document.id(), (document.view_id(), height));
    }

    let mut remote_needs = HashMap::new();

    for (document_id, (local_view_id, local_height)) in local_document_heights {
        if let Some((remote_view_id, remote_height, is_complete)) =
            remote_document_heights.get(document_id)
        {
            // If the local and remote view ids are the same then we know they're at the same
            // state and don't need to send anything.
            //
            // @TODO: We can handle this case in the logic above so as to not perform unnecessary
            // document height calculations.
            if local_view_id == remote_view_id {
                continue;
            }

            // The remote has branches we don't know about but we could still identify the
            // document from operations in the view id. We can't use the heights to compare state
            // now as not all branches are included in the calculation. We can send all our
            // operations to make sure we bring the remote up-to-date though.
            if !is_complete {
                // @TODO: I'm pretty sure this can be optimized, need to think it through a bit
                // more though.
                remote_needs.insert(document_id.to_owned(), 0_i32);
            };

            // If the height for the remote of this document couldn't be calculated then although
            // we know of the operations, we haven't materialized the document to this view yet,
            // so we can't send anything back to them.
            if remote_height.is_none() {
                continue;
            };

            // Safely unwrap as None case handled above.
            let remote_height = remote_height.unwrap();

            // If the remote height is less than the local height then we want to send them
            // all operations at an index greater than the remote height.
            if remote_height < local_height {
                remote_needs.insert(document_id.to_owned(), remote_height + 1);
                continue;
            }
        } else {
            // This document couldn't be identified from the remotes sent view ids. This can mean
            // two things:
            //
            // 1) The remote is further progressed than us so we can't identify the document yet
            //    as we don't have the operations.
            // 2) They don't know about this document yet.
            //
            // @TODO: With only sending view ids in Have messages we can't distinguish between
            // these two cases, it seems like we'll need to send the document id and view id.
            // There may be other solutions. Temporarily send all operations in both cases
            // although this is definitely not the behaviour we want and should be changed in this
            // PR.
            remote_needs.insert(document_id.to_owned(), 0_i32);
        };
    }

    remote_needs
}
