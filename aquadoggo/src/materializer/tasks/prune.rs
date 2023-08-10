// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::Human;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::{Task, TaskInput};

pub async fn prune_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    match input {
        TaskInput::DocumentId(id) => {
            // This task is concerned with a document which may now have dangling views. We want
            // to check for this and delete any views which are no longer needed.
            debug!("Prune document views for document: {}", id.display());

            // This method returns the document ids of child relations of any views which were deleted.
            let effected_child_relations = context
                .store
                .prune_document_views(&id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // We compose some more prune tasks based on the effected documents returned above.
            let next_tasks: Vec<Task<TaskInput>> = effected_child_relations
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

    use crate::materializer::tasks::prune_task;
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
            let next_tasks = prune_task(
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
            let next_tasks = prune_task(node.context.clone(), next_tasks[0].input().to_owned())
                .await
                .unwrap();

            // No more tasks should be issued.
            assert!(next_tasks.is_none());

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
