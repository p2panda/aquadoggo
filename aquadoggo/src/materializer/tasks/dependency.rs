// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::context::Context;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    let dependencies = context
        .store
        .get_document_view_dependencies(&input.document_view_id.unwrap())
        .await
        .map_err(|e| {
            println!("{:#?}", e);
            TaskError::Failure
        })?;

    let next_tasks: Vec<Task<TaskInput>> = dependencies
        .iter()
        .map(|relation_row| match relation_row.relation_type.as_str() {
            "relation" => Task::new(
                "reduce",
                TaskInput::new(Some(relation_row.value.parse().unwrap()), None),
            ),
            "relation_list" => Task::new(
                "reduce",
                TaskInput::new(Some(relation_row.value.parse().unwrap()), None),
            ),
            "pinned_relation" => Task::new(
                "reduce",
                TaskInput::new(None, Some(relation_row.value.parse().unwrap())),
            ),
            "pinned_relation_list" => Task::new(
                "reduce",
                TaskInput::new(None, Some(relation_row.value.parse().unwrap())),
            ),
            _ => panic!("Not a relation type"),
        })
        .collect();

    let next_tasks = if next_tasks.is_empty() {
        None
    } else {
        Some(next_tasks)
    };

    Ok(next_tasks)
}

#[cfg(test)]
mod tests {
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
    };
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
    async fn returns_dependency_task_inputs(
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
    async fn gets_parent_dependencies_as_well(
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

        let document_view_of_child = db
            .store
            .get_document_by_id(&document_id)
            .await
            .unwrap()
            .unwrap();

        let document_view_id_of_child = document_view_of_child.id();

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let reduce_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(reduce_tasks.len(), expected_next_tasks);

        let operation = create_operation(&[(
            "relation_to_existing_document",
            OperationValue::Relation(Relation::new(document_id.clone())),
        )]);
        let (_, document_view_id_of_parent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let reduce_tasks_of_child = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_parent.clone()));
        let reduce_tasks_of_parent = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(reduce_tasks_of_child.len(), expected_next_tasks + 1);
        assert_eq!(reduce_tasks_of_parent.len(), expected_next_tasks + 1);

        let operation = create_operation(&[(
            "parent_of_a_parent",
            OperationValue::PinnedRelation(PinnedRelation::new(document_view_id_of_parent.clone())),
        )]);
        let (_, document_view_id_of_grandparent) =
            insert_entry_operation_and_view(&db.store, &KeyPair::new(), None, &operation).await;

        let input = TaskInput::new(None, Some(document_view_id_of_child.clone()));
        let reduce_tasks_of_child = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_parent.clone()));
        let reduce_tasks_of_parent = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_grandparent));
        let reduce_tasks_of_grandparent = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(reduce_tasks_of_child.len(), expected_next_tasks + 2);
        assert_eq!(reduce_tasks_of_parent.len(), expected_next_tasks + 2);
        assert_eq!(reduce_tasks_of_grandparent.len(), expected_next_tasks + 2);
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
        let reduce_tasks = dependency_task(context.clone(), input)
            .await
            .unwrap()
            .unwrap();

        let input = TaskInput::new(None, Some(document_view_id_of_unrelated_document.clone()));
        let reduce_tasks_of_unrelated_document =
            dependency_task(context.clone(), input).await.unwrap();

        assert_eq!(reduce_tasks.len(), expected_next_tasks);
        assert!(reduce_tasks_of_unrelated_document.is_none());
    }
}
