// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::traits::AsOperation;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::Human;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::{Task, TaskInput};

pub async fn garbage_collection_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    match input {
        TaskInput::DocumentId(document_id) => {
            // This task is concerned with a document which may now have dangling views. We want
            // to check for this and delete any views which are no longer needed.
            debug!(
                "Prune document views for document: {}",
                document_id.display()
            );

            // Collect the ids of all views for this document.
            let all_document_view_ids: Vec<DocumentViewId> = context
                .store
                .get_all_document_view_ids(&document_id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // Iterate over all document views and delete them if no document view exists which refers
            // to it in a pinned relation field AND they are not the current view of a document.
            //
            // Deletes on "document_views" cascade to "document_view_fields" so rows there are also removed
            // from the database.
            let mut all_effected_child_relations = vec![];
            let mut deleted_views_count = 0;
            for document_view_id in &all_document_view_ids {
                // Check if this is the current view of it's document. This will still return true
                // if the document in question is deleted.
                let is_current_view = context
                    .store
                    .is_current_view(document_view_id)
                    .await
                    .map_err(|err| TaskError::Critical(err.to_string()))?;

                let mut effected_child_relations = vec![];
                let mut view_deleted = false;

                if !is_current_view {
                    // Before attempting to delete this view we need to fetch the ids of any child documents
                    // which might have views that could become unpinned as a result of this delete. These
                    // will be returned if the deletion is successful.
                    effected_child_relations = context
                        .store
                        .get_child_document_ids(document_view_id)
                        .await
                        .map_err(|err| TaskError::Critical(err.to_string()))?;

                    // Attempt to delete the view. If it is pinned from an existing view the deletion will
                    // not go ahead.
                    view_deleted = context
                        .store
                        .prune_document_view(document_view_id)
                        .await
                        .map_err(|err| TaskError::Critical(err.to_string()))?;
                }

                // If the view was deleted then push the effected children to the return array
                if view_deleted {
                    debug!("Deleted view: {}", document_view_id);
                    deleted_views_count += 1;
                    all_effected_child_relations.extend(effected_child_relations);
                } else {
                    debug!("Did not delete view: {}", document_view_id);
                }
            }

            // If the number of deleted views equals the total existing views (minus one for the
            // current view), then there is a chance this became completely detached. In this case
            // we should check if this document is a blob document and then try to purge it.
            if all_document_view_ids.len() - 1 == deleted_views_count {
                let operation = context
                    .store
                    .get_operation(&document_id.as_str().parse().unwrap())
                    .await
                    .map_err(|err| TaskError::Failure(err.to_string()))?
                    .expect("Operation exists in store");

                if let SchemaId::Blob(_) = operation.schema_id() {
                    // Purge the blob and all it's pieces. This only succeeds if no document
                    // refers to the blob document by either a relation or pinned relation.
                    context
                        .store
                        .purge_blob(&document_id)
                        .await
                        .map_err(|err| TaskError::Failure(err.to_string()))?;
                }
            }

            // We compose some more prune tasks based on the effected documents returned above.
            let next_tasks: Vec<Task<TaskInput>> = all_effected_child_relations
                .iter()
                .map(|document_id| {
                    debug!("Issue prune task for document: {document_id:#?}");
                    Task::new(
                        "garbage_collection",
                        TaskInput::DocumentId(document_id.to_owned()),
                    )
                })
                .collect();

            if next_tasks.is_empty() {
                Ok(None)
            } else {
                Ok(Some(next_tasks))
            }
        }
        _ => Err(TaskError::Critical("Invalid task input".into())),
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_view_id};
    use rstest::rstest;

    use crate::materializer::tasks::garbage_collection_task;
    use crate::materializer::{Task, TaskInput};
    use crate::test_utils::{
        add_blob, add_schema_and_documents, assert_query, test_runner, update_document, TestNode,
    };

    #[rstest]
    fn e2e_pruning(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish some documents which we will later point relations at.
            let (child_schema, child_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_child",
                vec![
                    vec![("uninteresting_field", 1.into(), None)],
                    vec![("uninteresting_field", 2.into(), None)],
                ],
                &key_pair,
            )
            .await;

            // Create some parent documents which contain a pinned relation list pointing to the
            // children created above.
            let (parent_schema, parent_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_parent",
                vec![vec![
                    ("name", "parent".into(), None),
                    (
                        "children",
                        child_document_view_ids.clone().into(),
                        Some(child_schema.id().to_owned()),
                    ),
                ]],
                &key_pair,
            )
            .await;

            // Convert view id to document id.
            let parent_document_id: DocumentId = parent_document_view_ids[0]
                .clone()
                .to_string()
                .parse()
                .unwrap();

            // Update the parent document so that there are now two views stored in the db, one
            // current and one dangling.
            let updated_parent_view_id = update_document(
                &mut node,
                parent_schema.id(),
                vec![("name", "Parent".into())],
                &parent_document_view_ids[0],
                &key_pair,
            )
            .await;

            // Get the historic (dangling) view to check it's actually there.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It is there...
            assert!(historic_document_view.is_some());

            // Create another document, which has a pinned relation to the parent document created
            // above. Now the relation graph looks like this
            //
            // GrandParent --> Parent --> Child1
            //                      \
            //                        --> Child2
            //
            let (schema_for_grand_parent, grand_parent_document_view_ids) =
                add_schema_and_documents(
                    &mut node,
                    "schema_for_grand_parent",
                    vec![vec![
                        ("name", "grand parent".into(), None),
                        (
                            "child",
                            parent_document_view_ids[0].clone().into(),
                            Some(parent_schema.id().to_owned()),
                        ),
                    ]],
                    &key_pair,
                )
                .await;

            // Convert view id to document id.
            let grand_parent_document_id: DocumentId = grand_parent_document_view_ids[0]
                .clone()
                .to_string()
                .parse()
                .unwrap();

            // Update the grand parent document to a new view, leaving the previous one dangling.
            //
            // Note: this test method _does not_ dispatch "garbage_collection" tasks.
            update_document(
                &mut node,
                schema_for_grand_parent.id(),
                vec![
                    ("name", "Grand Parent".into()),
                    ("child", updated_parent_view_id.into()),
                ],
                &grand_parent_document_view_ids[0],
                &key_pair,
            )
            .await;

            // Get the historic (dangling) view to make sure it exists.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&grand_parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It does...
            assert!(historic_document_view.is_some());

            // Now prune dangling views for the grand parent document. This method deletes any
            // dangling views (not pinned, not current) from the database for this document. It
            // returns the document ids of any documents which may have views which have become
            // "un-pinned" as a result of this view being removed. In this case, that's the
            // document id of the "parent" document.
            let next_tasks = garbage_collection_task(
                node.context.clone(),
                TaskInput::DocumentId(grand_parent_document_id),
            )
            .await
            .unwrap()
            .unwrap();

            // One new prune task is issued.
            assert_eq!(next_tasks.len(), 1);
            // It is the parent (which this grand parent relates to) as we expect.
            assert_eq!(
                next_tasks[0],
                Task::new(
                    "garbage_collection",
                    TaskInput::DocumentId(parent_document_id)
                )
            );

            // Check the historic view has been deleted.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&grand_parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It has...
            assert!(historic_document_view.is_none());

            // Now prune dangling views for the parent document.
            let next_tasks =
                garbage_collection_task(node.context.clone(), next_tasks[0].input().to_owned())
                    .await
                    .unwrap()
                    .unwrap();

            // Two new prune tasks issued.
            assert_eq!(next_tasks.len(), 2);
            // These are the two final child documents.
            assert_eq!(
                next_tasks,
                child_document_view_ids
                    .iter()
                    .rev()
                    .map(|document_view_id| {
                        let document_id: DocumentId = document_view_id.to_string().parse().unwrap();
                        Task::new("garbage_collection", TaskInput::DocumentId(document_id))
                    })
                    .collect::<Vec<Task<TaskInput>>>()
            );

            // Check the historic view has been deleted.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It has.
            assert!(historic_document_view.is_none());

            // Running the child tasks returns no new tasks.
            let next_tasks =
                garbage_collection_task(node.context.clone(), next_tasks[0].input().to_owned())
                    .await
                    .unwrap();

            assert!(next_tasks.is_none());
        });
    }

    #[rstest]
    fn no_new_tasks_issued_when_no_views_pruned(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Create a child document.
            let (child_schema, child_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_child",
                vec![vec![("uninteresting_field", 1.into(), None)]],
                &key_pair,
            )
            .await;

            // Create a parent document which contains a pinned relation list pointing to the
            // child created above.
            let (_, parent_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_parent",
                vec![vec![
                    ("name", "parent".into(), None),
                    (
                        "children",
                        child_document_view_ids.clone().into(),
                        Some(child_schema.id().to_owned()),
                    ),
                ]],
                &key_pair,
            )
            .await;

            // Run a garbage collection task for the parent.
            let document_id: DocumentId = parent_document_view_ids[0].to_string().parse().unwrap();
            let next_tasks =
                garbage_collection_task(node.context.clone(), TaskInput::DocumentId(document_id))
                    .await
                    .unwrap();

            // No views were pruned so we expect no new tasks to be issued.
            assert!(next_tasks.is_none());
        })
    }

    #[rstest]
    fn purges_blobs(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish a blob
            let blob_document_view = add_blob(
                &mut node,
                "Hello World!".as_bytes(),
                6,
                "text/plain",
                &key_pair,
            )
            .await;
            let blob_document_id: DocumentId = blob_document_view.to_string().parse().unwrap();

            // Check the blob is there
            let blob = node
                .context
                .store
                .get_blob(&blob_document_id)
                .await
                .unwrap();
            assert!(blob.is_some());

            // Run a garbage collection task for the blob document
            let next_tasks = garbage_collection_task(
                node.context.clone(),
                TaskInput::DocumentId(blob_document_id.clone()),
            )
            .await
            .unwrap();

            // It shouldn't return any new tasks
            assert!(next_tasks.is_none());

            // The blob should no longer be available
            let blob = node
                .context
                .store
                .get_blob(&blob_document_id)
                .await
                .unwrap();
            assert!(blob.is_none());

            // And all expected rows deleted from the database.
            assert_query(&node, "SELECT entry_hash FROM entries", 0).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 0).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 0).await;
            assert_query(&node, "SELECT log_id FROM logs", 3).await;
            assert_query(&node, "SELECT document_id FROM documents", 0).await;
            assert_query(&node, "SELECT document_id FROM document_views", 0).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 0).await;
        });
    }

    #[rstest]
    fn purges_newly_detached_blobs(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Create a blob document
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Relate to the blob from a new document
            let (schema, documents_pinning_blob) = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_view_id.clone().into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            // Now update the document to relate to another blob. This means the previously created
            // blob is now "dangling"
            update_document(
                &mut node,
                schema.id(),
                vec![("blob", random_document_view_id().into())],
                &documents_pinning_blob[0].clone(),
                &key_pair,
            )
            .await;

            // Run a task for the parent document
            let document_id: DocumentId = documents_pinning_blob[0].to_string().parse().unwrap();
            let next_tasks =
                garbage_collection_task(node.context.clone(), TaskInput::DocumentId(document_id))
                    .await
                    .unwrap()
                    .unwrap();

            // It issues one new task which is for the blob document
            assert_eq!(next_tasks.len(), 1);
            let next_tasks =
                garbage_collection_task(node.context.clone(), next_tasks[0].input().to_owned())
                    .await
                    .unwrap();
            // No new tasks issued
            assert!(next_tasks.is_none());

            // The blob has correctly been purged
            let blob = node
                .context
                .store
                .get_blob(&blob_document_id)
                .await
                .unwrap();

            assert!(blob.is_none());
        })
    }

    #[rstest]
    fn other_documents_keep_blob_alive(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Create a blob document.
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Relate to the blob from a new document.
            let (schema, documents_pinning_blob) = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_view_id.clone().into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            // Now update the document to relate to another blob. This means the previously
            // created blob is now "dangling".
            update_document(
                &mut node,
                schema.id(),
                vec![("blob", random_document_view_id().into())],
                &documents_pinning_blob[0].clone(),
                &key_pair,
            )
            .await;

            // Another document relating to the blob (this time from in a relation field).
            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_document_id.clone().into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            // Run a task for the parent document.
            let document_id: DocumentId = documents_pinning_blob[0].to_string().parse().unwrap();
            let next_tasks =
                garbage_collection_task(node.context.clone(), TaskInput::DocumentId(document_id))
                    .await
                    .unwrap()
                    .unwrap();

            // It issues one new task which is for the blob document.
            assert_eq!(next_tasks.len(), 1);
            let next_tasks =
                garbage_collection_task(node.context.clone(), next_tasks[0].input().to_owned())
                    .await
                    .unwrap();
            // No new tasks issued.
            assert!(next_tasks.is_none());

            // The blob should still be there as it was kept alive by a different document.
            let blob = node
                .context
                .store
                .get_blob(&blob_document_id)
                .await
                .unwrap();

            assert!(blob.is_some());
        })
    }

    #[rstest]
    fn all_relation_types_keep_blobs_alive(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();

            // Any type of relation can keep a blob alive, here we create one of each and run
            // garbage collection tasks for each blob.

            let blob_view_id_1 = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_view_id_1.clone().into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            let blob_view_id_2 = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    vec![blob_view_id_2.clone()].into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            let blob_view_id_3 = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_view_id_3
                        .to_string()
                        .parse::<DocumentId>()
                        .unwrap()
                        .into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            let blob_view_id_4 = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    vec![blob_view_id_4.to_string().parse::<DocumentId>().unwrap()].into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            for blob_view_id in [
                blob_view_id_1,
                blob_view_id_2,
                blob_view_id_3,
                blob_view_id_4,
            ] {
                let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
                let next_tasks = garbage_collection_task(
                    node.context.clone(),
                    TaskInput::DocumentId(blob_document_id.clone()),
                )
                .await
                .unwrap();

                assert!(next_tasks.is_none());

                // All blobs should be kept alive.
                let blob = node
                    .context
                    .store
                    .get_blob(&blob_document_id)
                    .await
                    .unwrap();

                assert!(blob.is_some());
            }
        })
    }
}