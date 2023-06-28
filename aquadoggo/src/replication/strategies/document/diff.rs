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

fn determine_document_height(document_view_operations: &Vec<StorageOperation>) -> Option<i32> {
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
        let (document_view_id_operations, is_complete) =
            get_document_view_id_operations(store, &document_view_id).await;

        for operation in &document_view_id_operations {
            if operation.document_id != document_id {
                panic!("They tricked us!!")
            }
        }

        let height = determine_document_height(&document_view_id_operations);

        remote_document_heights.insert(document_id, (document_view_id, height, is_complete));
    }

    // Calculate the document height for all passed local documents.
    let mut local_document_heights = HashMap::new();

    for document in &local_documents {
        let (document_view_id_operations, _) =
            get_document_view_id_operations(store, &document.view_id()).await;

        let height = determine_document_height(&document_view_id_operations)
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
                continue;
            };

            // If the height for the remote of this document couldn't be calculated then they are
            // more progressed than us and so we should do nothing.
            if remote_height.is_none() {
                continue;
            };

            // Safely unwrap as None case handled above.
            let remote_height = remote_height.unwrap();

            // If the remote height is less than the local height then we want to send them
            // all operations at an index greater than the remote height.
            if remote_height < local_height {
                remote_needs.insert(document_id.to_owned(), remote_height + 1);
            }
        } else {
            // The remote didn't know about this document yet so we send them everything we have.
            remote_needs.insert(document_id.to_owned(), 0_i32);
        };
    }

    remote_needs
}
