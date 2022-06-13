// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::OperationStore;

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

/// helper method for retrieving a document view by a documents' id and if it doesn't exist in
/// the store, composing a "reduce" task for this document.
async fn unpinned_relation_task(
    context: &Context,
    document_id: DocumentId,
) -> Result<Option<Task<TaskInput>>, TaskError> {
    match context
        .store
        .get_document_by_id(&document_id)
        .await
        .map_err(|_| TaskError::Critical)?
    {
        Some(_) => Ok(None),
        None => Ok(Some(Task::new(
            "reduce",
            TaskInput::new(Some(document_id), None),
        ))),
    }
}

pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    // PARSE INPUT ARGUMENTS //

    // Here we retrive the document view by document id or document view id depending on what was passed into
    // the task. This is needed so we can access the document view id and also check if a document has been
    // deleted. We also catch invalid task inputs here (both or neither ids were passed), failing the task
    // in a non critical way if this is the case.
    let (document_id, document_view) = match (&input.document_id, &input.document_view_id) {
        (Some(document_id), None) => {
            let document_view = context
                .store
                .get_document_by_id(document_id)
                .await
                .map_err(|_| TaskError::Critical)?;
            Ok((document_id.to_owned(), document_view))
        }
        // TODO: Alt approach: we could have a `get_operations_by_document_view_id()` in `OperationStore`, or
        // could we even do this with some fancy recursive SQL query? We might need the `previous_operations`
        // table back for that.
        (None, Some(document_view_id)) => {
            let document_view = context
                .store
                .get_document_view_by_id(document_view_id)
                .await
                .map_err(|_| TaskError::Critical)?;

            let operation_id = document_view_id.clone().into_iter().next().unwrap();
            match context
                .store
                .get_document_by_operation_id(operation_id)
                .await
                .map_err(|_| TaskError::Critical)? // We only get an error here on a critical database error.
            {
                Some(document_id) => Ok((document_id, document_view)),
                None => Err(TaskError::Failure),
            }
        }
        (_, _) => Err(TaskError::Failure),
    }?;

    let document_view = match document_view {
        Some(document_view) => document_view,
        // If no document view for the id passed into this task could be retrieved then either
        // it has been deleted or the id was somehow invalid. In that case we fail the task at this point.
        None => return Err(TaskError::Failure),
    };

    // FETCH DEPENDENCIES & COMPOSE TASKS //

    let mut child_tasks = Vec::new();
    let mut parent_tasks = Vec::new();

    // First we handle all pinned or unpinned relations defined in this tasks document view.
    // We can think of these "child" relations. We query the store for every child, if a view is
    // returned we do nothing. If it doesn't yet exist in the store (it hasn't been materialised)
    // we compose a "reduce" task for it.

    for (_key, document_view_value) in document_view.fields().iter() {
        match document_view_value.value() {
            p2panda_rs::operation::OperationValue::Relation(relation) => {
                child_tasks
                    .push(unpinned_relation_task(&context, relation.document_id().clone()).await?);
            }
            p2panda_rs::operation::OperationValue::RelationList(relation_list) => {
                for document_id in relation_list.iter() {
                    child_tasks.push(unpinned_relation_task(&context, document_id.clone()).await?);
                }
            }
            p2panda_rs::operation::OperationValue::PinnedRelation(pinned_relation) => {
                child_tasks
                    .push(pinned_relation_task(&context, pinned_relation.view_id().clone()).await?);
            }
            p2panda_rs::operation::OperationValue::PinnedRelationList(pinned_relation_list) => {
                for document_view_id in pinned_relation_list.iter() {
                    child_tasks
                        .push(pinned_relation_task(&context, document_view_id.clone()).await?);
                }
            }
            _ => (),
        }
    }

    // Next we want to find any existing documents in the store which relate to this document view OR document
    // themselves. We do this for both pinned and unpinned relations incase this is the first time the tasks
    // document is being materialised. Here we dispatch a "dependency" task for any documents found.
    context
        .store
        .get_parents_with_unpinned_relation(&document_id)
        .await
        .map_err(|_| TaskError::Critical)?
        .iter()
        .for_each(|document_view_id| {
            parent_tasks.push(Some(Task::new(
                "dependency",
                TaskInput::new(None, Some(document_view_id.clone())),
            )))
        });

    context
        .store
        .get_parents_with_pinned_relation(document_view.id())
        .await
        .map_err(|_| TaskError::Critical)?
        .into_iter()
        .for_each(|document_view_id| {
            parent_tasks.push(Some(Task::new(
                "dependency",
                TaskInput::new(None, Some(document_view_id)),
            )))
        });

    let mut next_tasks = Vec::new();
    let mut child_tasks: Vec<Task<TaskInput>> = child_tasks.into_iter().flatten().collect();
    let mut parent_tasks: Vec<Task<TaskInput>> = parent_tasks.into_iter().flatten().collect();

    if child_tasks.is_empty() {
        // This means all dependencies this document view relates to are met and we
        // should dispatch a schema task. We also dispatch all parent dependency tasks
        // incase they now have all their dependencies met.
        next_tasks.append(&mut vec![Task::new(
            "schema",
            TaskInput::new(None, Some(document_view.id().clone())),
        )]);
        next_tasks.append(&mut parent_tasks);
    } else {
        // If not all dependencies were met, then we want to dispatch all children and parent tasks.
        next_tasks.append(&mut child_tasks);
        next_tasks.append(&mut parent_tasks);
    };

    Ok(Some(next_tasks))
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
        vec![]), 1)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("favorite_book_images", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 6)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("something_from_the_past", OperationValue::PinnedRelation(PinnedRelation::new(random_document_view_id())))],
        vec![]), 1)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("many_previous_drafts", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect())))],
        vec![]), 2)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 8)]
    // This document has been updated
    #[case(test_db(4, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 10].iter().map(|_|random_document_id()).collect())))],
    ), 12)]
    #[tokio::test]
    async fn dispatches_reduce_tasks_for_child_dependencies(
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
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
        vec![]), 1)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 8)]
    #[case(test_db(4, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 10].iter().map(|_|random_document_id()).collect())))],
    ), 12)]
    #[tokio::test]
    async fn dispatches_task_for_parent_dependencies_as_well(
        #[case]
        #[future]
        db: TestSqlStore,
        #[case] expected_reduce_tasks: usize,
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

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tasks.len(), expected_reduce_tasks);
        for task in tasks {
            assert_eq!(task.0, "reduce")
        }

        // Create a new document referencing the existing materialised document.

        let operation = create_operation(&[(
            "relation_to_existing_document",
            OperationValue::Relation(Relation::new(document_id.clone())),
        )]);
        let (_, document_view_id_of_parent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        // The child should now dispatch one dependency task for the parent as well as
        // reduce tasks for it's children.

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let child_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let expected_task_types = [("reduce", expected_reduce_tasks), ("dependency", 1)];
        assert_eq!(child_tasks.len(), expected_reduce_tasks + 1);
        for (task_type, count) in expected_task_types {
            assert_eq!(
                child_tasks
                    .iter()
                    .filter(|task| task.0 == task_type)
                    .count(),
                count
            );
        }

        // The parent should dispatch one schema task as it has one dependency which was already materialised.

        let input = TaskInput::new(None, Some(document_view_id_of_parent.clone()));
        let parent_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(parent_tasks.len(), 1);
        assert_eq!(parent_tasks[0].0, "schema");

        // Now we create another document with a pinned relation to the parent and a pinned relation
        // list with an item pointing to the child document.

        let operation = create_operation(&[
            (
                "parent_of",
                OperationValue::PinnedRelation(PinnedRelation::new(
                    document_view_id_of_parent.clone(),
                )),
            ),
            (
                "grandparent_of",
                OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                    document_view_id_of_child.clone(),
                ])),
            ),
        ]);
        let (_, document_view_id_of_grandparent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let child_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_parent.clone()));
        let parent_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_grandparent));
        let grandparent_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        // The child dispatches an extra dependency task for it's grand parent.

        let expected_dependency_tasks = 2;
        let expected_task_types = [
            ("reduce", expected_reduce_tasks),
            ("dependency", expected_dependency_tasks),
        ];
        assert_eq!(
            child_tasks.len(),
            expected_reduce_tasks + expected_dependency_tasks
        );
        for (task_type, count) in expected_task_types {
            assert_eq!(
                child_tasks
                    .iter()
                    .filter(|task| task.0 == task_type)
                    .count(),
                count
            );
        }

        // The parent dispatches a dependency task for it's parent and schema task for itself.

        let expected_schema_tasks = 1;
        let expected_dependency_tasks = 1;
        let expected_task_types = [
            ("schema", expected_schema_tasks),
            ("dependency", expected_dependency_tasks),
        ];
        assert_eq!(
            parent_tasks.len(),
            expected_schema_tasks + expected_dependency_tasks
        );
        for (task_type, count) in expected_task_types {
            assert_eq!(
                parent_tasks
                    .iter()
                    .filter(|task| task.0 == task_type)
                    .count(),
                count
            );
        }

        // The grandparent dispatches one schema task as all it's dependencies are met.

        let expected_schema_tasks = 1;
        assert_eq!(grandparent_tasks.len(), expected_schema_tasks);
        assert_eq!(
            grandparent_tasks
                .iter()
                .filter(|task| task.0 == "schema")
                .count(),
            expected_schema_tasks
        );
    }

    #[rstest]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
        vec![]), 1)]
    #[case(test_db(1, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![]), 8)]
    #[case(test_db(4, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 6].iter().map(|_|random_document_id()).collect())))],
        vec![("one_relation_field", OperationValue::PinnedRelationList(PinnedRelationList::new([0; 2].iter().map(|_|random_document_view_id()).collect()))),
             ("another_relation_field", OperationValue::RelationList(RelationList::new([0; 10].iter().map(|_|random_document_id()).collect())))],
    ), 12)]
    #[tokio::test]
    async fn ignores_fields_which_look_like_relations_but_are_not(
        #[case]
        #[future]
        db: TestSqlStore,
        #[case] expected_next_tasks: usize,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());
        let document_id = db.documents[0].clone();

        let input = TaskInput::new(Some(document_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap().unwrap();

        let document_view = db
            .store
            .get_document_by_id(&document_id)
            .await
            .unwrap()
            .unwrap();

        let document_view_id = document_view.id();

        let input = TaskInput::new(None, Some(document_view_id.clone()));

        dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let operation = create_operation(&[(
            "not_a_relation_but_contains_a_hash",
            OperationValue::Text(document_id.as_str().to_string()),
        )]);

        let (_, document_view_id_of_unrelated_document) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let input = TaskInput::new(None, Some(document_view_id.clone()));
        let tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_unrelated_document.clone()));
        let tasks_of_unrelated_document = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tasks.len(), expected_next_tasks);
        assert_eq!(tasks_of_unrelated_document.len(), 1);
        assert_eq!(tasks_of_unrelated_document[0].0, "schema");
    }

    #[rstest]
    #[should_panic(expected = "Failure")]
    #[case(None, Some(random_document_view_id()))]
    #[should_panic(expected = "Failure")]
    #[case(None, None)]
    #[should_panic(expected = "Failure")]
    #[case(Some(random_document_id()), None)]
    #[should_panic(expected = "Failure")]
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
    #[should_panic(expected = "Failure")]
    #[case(test_db(2, 1, true, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
        vec![]))]
    #[should_panic(expected = "Failure")]
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

    #[rstest]
    #[tokio::test]
    async fn gets_parent_relations(
        #[from(test_db)]
        #[future]
        #[with(1, 1)]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());
        let document_id = db.documents[0].clone();

        let input = TaskInput::new(Some(document_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap().unwrap();

        // Here we have one materialised document, (we are calling it a child as we will shortly be publishing parents).

        let document_view_of_child = db
            .store
            .get_document_by_id(&document_id)
            .await
            .unwrap()
            .unwrap();

        let document_view_id_of_child = document_view_of_child.id();

        // Create a new document referencing the existing materialised document by unpinned relation.

        let operation = create_operation(&[(
            "relation_to_existing_document",
            OperationValue::Relation(Relation::new(document_id.clone())),
        )]);
        let (_, document_view_id_of_parent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let parent_dependencies_unpinned = db
            .store
            .get_parents_with_unpinned_relation(&document_id)
            .await
            .unwrap();
        assert_eq!(parent_dependencies_unpinned.len(), 1);
        assert_eq!(parent_dependencies_unpinned[0], document_view_id_of_parent);

        // Create a new document referencing the existing materialised document by pinned relation.

        let operation = create_operation(&[(
            "pinned_relation_to_existing_document",
            OperationValue::PinnedRelation(PinnedRelation::new(document_view_id_of_child.clone())),
        )]);
        let (_, document_view_id_of_parent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let parent_dependencies_pinned = db
            .store
            .get_parents_with_pinned_relation(document_view_id_of_child)
            .await
            .unwrap();
        assert_eq!(parent_dependencies_pinned.len(), 1);
        assert_eq!(parent_dependencies_pinned[0], document_view_id_of_parent);
    }
}
