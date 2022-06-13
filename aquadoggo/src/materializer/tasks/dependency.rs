// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentViewId;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// helper method for retrieving a document view by it's document view id and if it doesn't exist in
/// the store, composing a "reduce" task for this specific document view.
async fn pinned_relation_task(
    context: &Context,
    document_view_id: DocumentViewId,
) -> Result<Option<Task<TaskInput>>, TaskError> {
    match context
        .store
        .get_document_view_by_id(&document_view_id)
        .await
        .map_err(|_| TaskError::Critical)?
    {
        Some(_) => Ok(None),
        None => Ok(Some(Task::new(
            "reduce",
            TaskInput::new(None, Some(document_view_id)),
        ))),
    }
}

pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    // PARSE INPUT ARGUMENTS //

    // Here we retrive the document view by document id or document view id depending on what was passed into
    // the task. This is needed so we can access the document view id and also check if a document has been
    // deleted. We also catch invalid task inputs here (both or neither ids were passed), failing the task
    // in a non critical way if this is the case.
    let document_view = match (&input.document_id, &input.document_view_id) {
        // TODO: Alt approach: we could have a `get_operations_by_document_view_id()` in `OperationStore`, or
        // could we even do this with some fancy recursive SQL query? We might need the `previous_operations`
        // table back for that.
        (None, Some(document_view_id)) => {
            // Fetch the document_view from the store by it's document view id.
            context
                .store
                .get_document_view_by_id(document_view_id)
                .await
                .map_err(|_| TaskError::Critical)
        }
        (_, _) => Err(TaskError::Critical),
    }?;

    let document_view = match document_view {
        Some(document_view) => document_view,
        // If no document view for the id passed into this task could be retrieved then this
        // document has been deleted or the document view id was invalid. As "dependency" tasks
        // are only dispatched after a successful "reduce" task, neither `None` case should
        // happen, so this is a critical error.
        None => return Err(TaskError::Critical),
    };

    // FETCH DEPENDENCIES & COMPOSE TASKS //

    let mut next_tasks = Vec::new();

    // First we handle all pinned or unpinned relations defined in this tasks document view.
    // We can think of these as "child" relations.
    for (_key, document_view_value) in document_view.fields().iter() {
        match document_view_value.value() {
            p2panda_rs::operation::OperationValue::Relation(_relation) => {
                // This is a relation to a document, if it doesn't exist in the db yet, then that
                // means we either have no entries for this document, or we are not materialising
                // it for some reason. We don't want to kick of a "reduce" or "dependency" task in
                // either of these cases, however in the future we may want to flag it as a "wanted"
                // schema.
            }
            p2panda_rs::operation::OperationValue::RelationList(_relation_list) => {
                // same as above...
            }
            p2panda_rs::operation::OperationValue::PinnedRelation(pinned_relation) => {
                // These are pinned relations. We may have the operations for these views in the db,
                // but this view wasn't pinned yet, so hasn't been materialised. To make sure it is
                // materialised when possible, we dispatch a "reduce" task for any pinned relations
                // which aren't found.
                next_tasks
                    .push(pinned_relation_task(&context, pinned_relation.view_id().clone()).await?);
            }
            p2panda_rs::operation::OperationValue::PinnedRelationList(pinned_relation_list) => {
                // same as above...
                for document_view_id in pinned_relation_list.iter() {
                    next_tasks
                        .push(pinned_relation_task(&context, document_view_id.clone()).await?);
                }
            }
            _ => (),
        }
    }

    Ok(Some(next_tasks.into_iter().flatten().collect()))
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
    };
    use p2panda_rs::storage_provider::traits::{AsStorageOperation, OperationStore};
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{
        create_operation, random_document_id, random_document_view_id,
    };
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{insert_entry_operation_and_view, test_db, TestSqlStore};
    use crate::db::traits::DocumentStore;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;

    use super::dependency_task;

    #[rstest]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
        vec![]), 0)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("favorite_book_images", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 0)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("something_from_the_past", OperationValue::PinnedRelation(PinnedRelation::new(random_document_view_id())))],
        vec![]), 1)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("many_previous_drafts", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect())))],
        vec![]), 2)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 2)]
    // This document has been updated
    #[case(test_db(4, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 3].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 10].iter().map(|_|random_document_id()).collect())))],
    ), 3)]
    #[tokio::test]
    async fn dispatches_reduce_tasks_for_pinned_child_dependencies(
        #[case]
        #[future]
        db: TestSqlStore,
        #[case] expected_next_tasks: usize,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());

        for document_id in &db.documents {
            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(context.clone(), input).await.unwrap().unwrap();
        }

        for document_id in &db.documents {
            let document_view = db
                .store
                .get_document_by_id(document_id)
                .await
                .unwrap()
                .unwrap();

            let input = TaskInput::new(None, Some(document_view.id().clone()));

            let reduce_tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reduce_tasks.len(), expected_next_tasks);
            for task in reduce_tasks {
                assert_eq!(task.0, "reduce")
            }
        }
    }

    #[rstest]
    #[tokio::test]
    async fn no_reduce_task_for_materialised_document_relations(
        #[from(test_db)]
        #[with(1, 1)]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());
        let document_id = db.documents[0].clone();

        let input = TaskInput::new(Some(document_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap().unwrap();

        // Here we have one materialised document, (we are calling it a child as we will shortly be publishing parents)
        // it contains relations which are not materialised yet so should dispatch a reduce task for each one.

        let document_view_of_child = db
            .store
            .get_document_by_id(&document_id)
            .await
            .unwrap()
            .unwrap();

        let document_view_id_of_child = document_view_of_child.id();

        // Create a new document referencing the existing materialised document.

        let operation = create_operation(&[
            (
                "pinned_relation_to_existing_document",
                OperationValue::PinnedRelation(PinnedRelation::new(
                    document_view_id_of_child.clone(),
                )),
            ),
            (
                "pinned_relation_to_not_existing_document",
                OperationValue::PinnedRelation(PinnedRelation::new(random_document_view_id())),
            ),
        ]);

        let (_, document_view_id) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        // The new document should now dispatch one dependency task for the child relation which
        // has not been materialised yet.
        let input = TaskInput::new(None, Some(document_view_id.clone()));
        let tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0, "reduce");
    }

    #[rstest]
    #[should_panic(expected = "Critical")]
    #[case(None, Some(random_document_view_id()))]
    #[should_panic(expected = "Critical")]
    #[case(None, None)]
    #[should_panic(expected = "Critical")]
    #[case(Some(random_document_id()), None)]
    #[should_panic(expected = "Critical")]
    #[case(Some(random_document_id()), Some(random_document_view_id()))]
    #[tokio::test]
    async fn fails_correctly(
        #[case] document_id: Option<DocumentId>,
        #[case] document_view_id: Option<DocumentViewId>,
        #[from(test_db)]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store, Configuration::default());
        let input = TaskInput::new(document_id, document_view_id);

        let next_tasks = dependency_task(context.clone(), input).await.unwrap();
        assert!(next_tasks.is_none())
    }

    #[rstest]
    #[should_panic(expected = "Critical")]
    #[case(test_db(2, 1, true, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
        vec![]))]
    #[should_panic(expected = "Critical")]
    #[case(test_db(2, 1, true, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]))]
    #[tokio::test]
    async fn fails_on_deleted_documents(
        #[case]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());
        let document_id = db.documents[0].clone();

        let input = TaskInput::new(Some(document_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap();

        let document_operations = db
            .store
            .get_operations_by_document_id(&document_id)
            .await
            .unwrap();

        let document_view_id: DocumentViewId = document_operations[1].id().into();

        let input = TaskInput::new(None, Some(document_view_id.clone()));

        dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();
    }
}
