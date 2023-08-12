// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::traits::AsOperation;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
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
                // Check if this is the current view of it's document.
                let is_current_view = context
                    .store
                    .is_current_view(&document_view_id)
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
                        .get_child_document_view_ids(document_view_id)
                        .await
                        .map_err(|err| TaskError::Critical(err.to_string()))?;

                    // Attempt to delete the view. If it is pinned from an existing view the deletion will
                    // not go ahead.
                    view_deleted = context
                        .store
                        .prune_document_view(&document_view_id)
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

            if all_document_view_ids.len() == deleted_views_count {
                let operation = context
                    .store
                    .get_operation(&document_id.as_str().parse().unwrap())
                    .await
                    .map_err(|err| TaskError::Failure(err.to_string()))?
                    .expect("Operation exists in store");

                if let SchemaId::Blob(_) = operation.schema_id() {
                    // @TODO: we should purge blob and all blob piece documents too.
                    context
                        .store
                        .purge_document(&document_id)
                        .await
                        .map_err(|err| TaskError::Failure(err.to_string()))?;
                }
            }

            // We compose some more prune tasks based on the effected documents returned above.
            let next_tasks: Vec<Task<TaskInput>> = all_effected_child_relations
                .iter()
                .map(|document_id| {
                    debug!("Issue prune task for document: {document_id:#?}");
                    Task::new("prune", TaskInput::DocumentId(document_id.to_owned()))
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
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::materializer::tasks::garbage_collection_task;
    use crate::materializer::{Task, TaskInput};
    use crate::test_utils::{add_schema_and_documents, test_runner, update_document, TestNode};

    #[rstest]
    fn prunes_document_views(key_pair: KeyPair) {
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
            // Note: this method _does not_ dispatch "prune" tasks.
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
                Task::new("prune", TaskInput::DocumentId(parent_document_id))
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
                        Task::new("prune", TaskInput::DocumentId(document_id))
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
        });
    }
}
