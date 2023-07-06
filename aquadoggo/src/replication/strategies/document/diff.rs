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

fn compare_view_ids(received_view_id: &DocumentViewId, expected_view_id: &DocumentViewId) -> bool {
    expected_view_id.iter().all(|item| received_view_id.graph_tips().contains(item))
}

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

/// Determine the document_height and expected document_view_id for the passed document view id.
///
/// This retrieves the operations identified in the view id, iterates over them and returns the
/// highest `sorted_index` and `document_view_id` at that point in the documents history. In the
/// case where the operation has not been reduced as part of a view yet, both values will
/// be none. If this is the case for all operations in the view then the document state will be
/// `None`.
///
/// @TODO: This can be optimized with a single `get_document_height_by_view_id()` method on the store.
async fn determine_document_state(
    store: &SqlStore,
    remote_document_view_id: &DocumentViewId,
) -> Option<(i32, DocumentViewId)> {
    let mut local_document_state = None::<(i32, DocumentViewId)>;

    let document_view_id_operations =
        get_document_view_id_operations(store, remote_document_view_id).await;

    for operation in &document_view_id_operations {
        local_document_state = match (operation.sorted_index, operation.clone().document_view_id) {
            (Some(index), Some(document_view_id)) => {
                let is_greater = local_document_state
                    .as_ref()
                    .map_or(true, |(current_index, _)| current_index < &index);

                if is_greater {
                    Some((index, document_view_id))
                } else {
                    local_document_state
                }
            }
            (_, _) => local_document_state,
        };
    }

    local_document_state
}

/// Compare a set of remote documents (identified by their document id and view id) with the
/// documents we have in our local store for the passed target set. Return a map of document ids
/// and heights signifying from where in the document the remote peer needs to be updated in order
/// to bring them in line with our current state.
pub async fn diff_documents(
    store: &SqlStore,
    target_set: &TargetSet,
    remote_documents: &[(DocumentId, DocumentViewId)],
) -> HashMap<DocumentId, i32> {
    let remote_documents: HashMap<DocumentId, DocumentViewId> =
        remote_documents.iter().cloned().collect();

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
    // 3) If they know our document but we can't calculate their document height, then they are
    //    have operations to send us.
    // 4) If the received view id doesn't contain all tips from the expected view id then they are
    //    missing a branch and we send all operations for the document to bring them up-to-date
    //    with us.
    // 5) Finally we know they aren't missing any branches and so we can compare document heights
    //    and send only the new operations the remote needs to get up to date.
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

            // Calculate the remote document state. This includes getting the locally known height
            // and expected document view.
            let remote_document_state =
                determine_document_state(store, remote_document_view_id).await;

            // If the state for the remote of this document couldn't be calculated then they are
            // further progressed for this document and so we don't need to send them anything.
            if remote_document_state.is_none() {
                trace!(
                    "Could not calculate remote document height {} <DocumentViewId {}>: no action required",
                    document_id.display(),
                    remote_document_view_id.display()
                );
                // Continue to loop over the remaining documents.
                continue;
            };

            // Safely unwrap as None case handled above.
            let (remote_height, expected_remote_view_id) = remote_document_state.unwrap();

            trace!(
                "Remote document height: {} <DocumentViewId {}> {remote_height}",
                local_document.id().display(),
                remote_document_view_id.display()
            );

            // Calculate the local document state.
            //
            // @TODO: This could be made more efficient if we store the current document height in the
            // store.
            let local_document_state =
                determine_document_state(store, local_document.view_id()).await;

            // Unwrap as we know local documents have been materialized, we only care about the
            // document height here.
            let (local_height, _) =
                local_document_state.expect("All local documents have been materialized");

            trace!(
                "Local document height: {} <DocumentViewId {}> {local_height:?}",
                local_document.id().display(),
                local_document.view_id().display()
            );

            // Check that the expected view id for the remote document is a sub-set of the
            // received view id. This means they know about all branches we have locally at this
            // point in the document.
            if compare_view_ids(remote_document_view_id, &expected_remote_view_id) {
                trace!("Remote view id is missing branches: send all operations");
                remote_needs.insert(local_document.id().to_owned(), 0);
            }

            // Finally here we know that that the remote and local contain the same past state,
            // and so here we can calculate only the new operations which we should send to the remote.
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

#[cfg(test)]
mod tests {
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationAction, OperationBuilder};
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::memory_store::helpers::{send_to_store, PopulateStoreConfig};
    use rstest::rstest;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::replication::strategies::document::diff_documents;
    use crate::replication::TargetSet;
    use crate::test_utils::{
        populate_and_materialize, populate_store_config, test_runner_with_manager, TestNode,
        TestNodeManager,
    };

    async fn node_has(
        node: &TestNode,
        target_set: &TargetSet,
    ) -> Vec<(DocumentId, DocumentViewId)> {
        let mut documents = Vec::new();
        for schema_id in target_set.iter() {
            let schema_documents = node
                .context
                .store
                .get_documents_by_schema(schema_id)
                .await
                .expect("Fatal database error");
            documents.extend(
                schema_documents
                    .into_iter()
                    .map(|document| (document.id().to_owned(), document.view_id().to_owned())),
            );
        }
        documents
    }

    #[rstest]
    fn does_diff_documents(
        #[from(populate_store_config)]
        #[with(10, 2, 1)]
        config_a: PopulateStoreConfig,
        #[from(populate_store_config)]
        #[with(5, 4, 1)]
        config_b: PopulateStoreConfig,
        #[from(populate_store_config)] config_c: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let schema_id = config_a.schema.id();
            let target_set = TargetSet::new(&[schema_id.to_owned()]);

            // Create three nodes.
            let mut node_a = manager.create().await;
            let mut node_b = manager.create().await;
            let mut node_c = manager.create().await;

            // Populate them each with documents.
            populate_and_materialize(&mut node_a, &config_a).await;
            populate_and_materialize(&mut node_b, &config_b).await;
            populate_and_materialize(&mut node_c, &config_c).await;

            // Compose the state of each node in the method param format.
            let node_a_has = node_has(&node_a, &target_set).await;
            let node_b_has = node_has(&node_b, &target_set).await;
            let node_c_has = node_has(&node_c, &target_set).await;

            // Diffing nodes against their own state should return empty maps.
            let remote_needs =
                diff_documents(&node_a.context.store, &target_set, &node_a_has).await;
            assert!(remote_needs.is_empty());

            let remote_needs =
                diff_documents(&node_b.context.store, &target_set, &node_b_has).await;
            assert!(remote_needs.is_empty());

            let remote_needs =
                diff_documents(&node_c.context.store, &target_set, &node_c_has).await;
            assert!(remote_needs.is_empty());

            // Node C has no documents, they need operations for all documents present on other nodes.
            let remote_needs =
                diff_documents(&node_a.context.store, &target_set, &node_c_has).await;

            assert_eq!(remote_needs.len(), node_a_has.len());
            for (_, height) in remote_needs {
                assert_eq!(height, 0);
            }

            // Node B has all of Node A's documents, but needs new operations from height 5 and above.
            let node_b_needs_from_node_a =
                diff_documents(&node_a.context.store, &target_set, &node_b_has).await;

            let expected_needs = node_a_has
                .clone()
                .into_iter()
                .map(|(document_id, _)| (document_id, 5))
                .collect();

            assert_eq!(node_b_needs_from_node_a.len(), 2);
            assert_eq!(node_b_needs_from_node_a, expected_needs);

            // Node A is needs all operations for 2 new documents from Node B.
            let mut node_a_needs_from_node_b =
                diff_documents(&node_b.context.store, &target_set, &node_a_has)
                    .await
                    .into_iter();

            assert_eq!(node_a_needs_from_node_b.len(), 2);
            assert!(matches!(node_a_needs_from_node_b.next(), Some((_, 0))));
            assert!(matches!(node_a_needs_from_node_b.next(), Some((_, 0))));

            // Node A doesn't need anything from Node C (because it is empty)
            let remote_needs =
                diff_documents(&node_c.context.store, &target_set, &node_a_has).await;
            assert!(remote_needs.is_empty());
        })
    }

    #[rstest]
    fn one_node_missing_branch(
        #[from(populate_store_config)]
        #[with(10, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let schema = config.schema.clone();
            let target_set = TargetSet::new(&[schema.id().to_owned()]);

            // Create three nodes.
            let mut node_a = manager.create().await;
            let mut node_b = manager.create().await;

            // Populate them each with documents.
            let (_, documents) = populate_and_materialize(&mut node_a, &config).await;
            populate_and_materialize(&mut node_b, &config).await;

            // Compose the state of node b in the method param format.
            let node_b_has = node_has(&node_b, &target_set).await;

            // Publish an update into this documents past.
            let document_id = documents[0].clone();

            let update_operation = OperationBuilder::new(schema.id())
                .action(OperationAction::Update)
                .previous(&document_id.as_str().parse().unwrap())
                .fields(&[("username", "よつばと".into())])
                .build()
                .unwrap();

            send_to_store(
                &node_a.context.store,
                &update_operation,
                &schema,
                &KeyPair::new(),
            )
            .await
            .unwrap();

            // Reduce the updated document.
            let input = TaskInput::DocumentId(document_id.clone());
            let _ = reduce_task(node_a.context.clone(), input).await.unwrap();

            // Diff between nodes.
            let node_b_needs_from_node_a =
                diff_documents(&node_a.context.store, &target_set, &node_b_has).await;

            // Node A identified that node B is missing a branch and so sends them all documents
            // to keep them up to date.
            assert_eq!(
                node_b_needs_from_node_a,
                vec![(document_id, 10)].into_iter().collect()
            );

            // Now we do it the other way around.

            // Compose the state of node a in the method param format.
            let node_a_has = node_has(&node_b, &target_set).await;

            // Diff between nodes.
            let node_a_needs_from_node_a =
                diff_documents(&node_b.context.store, &target_set, &node_a_has).await;

            // Node B identified that node A is is up to date, even though they sent them a
            // document view id containing a branch they don't know about.
            assert_eq!(
                node_a_needs_from_node_a,
                vec![].into_iter().collect()
            );
        })
    }
}
