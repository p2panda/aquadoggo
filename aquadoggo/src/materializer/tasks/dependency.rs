// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::context::Context;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    let dependencies = context
        .store
        .get_document_view_dependencies(&input.document_view_id.unwrap())
        .await
        .map_err(|_| TaskError::Failure)?;

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
    use p2panda_rs::operation::{
        OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
    };
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{random_document_id, random_document_view_id};
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{test_db, TestSqlStore};
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
}
