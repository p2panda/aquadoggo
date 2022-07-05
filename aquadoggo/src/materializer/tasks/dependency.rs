// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A dependency task prepares _reduce_ tasks for all pinned relations of a given document view.
///
/// This task is dispatched after a reduce task completes. It identifies any pinned relations
/// present in a given document view as we need to guarantee the required document views are
/// materialised and stored in the database. We may have the required operations on the node
/// already, but they were never materialised to the document view required by the pinned
/// relation. In order to guarantee all required document views are present we dispatch a
/// reduce task for the view of each pinned relation found.
///
/// Expects a _reduce_ task to have completed successfully for the given document view itself and
/// returns a critical error otherwise.
pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on dependency task {:#?}", input);

    // Here we retrive the document view by document view id.
    let document_view = match &input.document_view_id {
        Some(view_id) => context
            .store
            .get_document_view_by_id(view_id)
            .await
            .map_err(|err| {
                TaskError::Critical(err.to_string())
            })
            ,
        // We expect to handle document_view_ids in a dependency task.
        None => Err(TaskError::Critical("Missing document_view_id in task input".into())),
    }?;

    let document_view = match document_view {
        Some(document_view) => {
            debug!(
                "Document view retrieved from storage with id: {}",
                document_view.id()
            );
            Ok(document_view)
        }
        // If no document view for the id passed into this task could be retrieved then this
        // document has been deleted or the document view id was invalid. As "dependency" tasks
        // are only dispatched after a successful "reduce" task, neither `None` case should
        // happen, so this is a critical error.
        None => Err(TaskError::Critical(format!(
            "Expected document view {} not found in store",
            &input.document_view_id.unwrap()
        ))),
    }?;

    let mut next_tasks = Vec::new();

    // First we handle all pinned or unpinned relations defined in this task's document view.
    // We can think of these as "child" relations.
    for (_key, document_view_value) in document_view.fields().iter() {
        match document_view_value.value() {
            p2panda_rs::operation::OperationValue::Relation(_relation) => {
                // This is a relation to a document, if it doesn't exist in the db yet, then that
                // means we either have no entries for this document, or we are not materialising
                // it for some reason. We don't want to kick of a "reduce" or "dependency" task in
                // either of these cases.
                debug!("Relation field found, no action required.");
            }
            p2panda_rs::operation::OperationValue::RelationList(_relation_list) => {
                // same as above...
                debug!("Relation list field found, no action required.");
            }
            p2panda_rs::operation::OperationValue::PinnedRelation(pinned_relation) => {
                // These are pinned relations. We may have the operations for these views in the db,
                // but this view wasn't pinned yet, so hasn't been materialised. To make sure it is
                // materialised when possible, we dispatch a "reduce" task for any pinned relations
                // which aren't found.
                debug!(
                    "Pinned relation field found refering to view id: {}",
                    pinned_relation.view_id()
                );
                next_tasks.push(
                    construct_relation_task(&context, pinned_relation.view_id().clone()).await?,
                );
            }
            p2panda_rs::operation::OperationValue::PinnedRelationList(pinned_relation_list) => {
                // same as above...
                for document_view_id in pinned_relation_list.iter() {
                    debug!(
                        "Pinned relation list field found containing view id: {}",
                        document_view_id
                    );
                    next_tasks
                        .push(construct_relation_task(&context, document_view_id.clone()).await?);
                }
            }
            _ => (),
        }
    }

    // Construct additional tasks if the task input matches certain system schemas and all
    // dependencies have been reduced.
    if !next_tasks.iter().any(|task| task.is_some()) {
        let task_input_schema = context
            .store
            .get_schema_by_document_view(document_view.id())
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?
            // Unwrap because we expect the task input to still be in the db.
            .unwrap();

        // Helper that returns a schema task for the current task input.
        let schema_task = || {
            Some(Task::new(
                "schema",
                TaskInput::new(None, Some(document_view.id().clone())),
            ))
        };

        match task_input_schema {
            // Start `schema` task when a schema (field) definition view is completed with
            // dependencies
            SchemaId::SchemaDefinition(_) => next_tasks.push(schema_task()),
            SchemaId::SchemaFieldDefinition(_) => next_tasks.push(schema_task()),
            _ => {}
        }
    }

    debug!(
        "Scheduling {} reduce tasks",
        next_tasks.iter().filter(|t| t.is_some()).count()
    );

    Ok(Some(next_tasks.into_iter().flatten().collect()))
}

/// Returns a _reduce_ task for a given document view only if that view does not yet exist in the
/// store.
async fn construct_relation_task(
    context: &Context,
    document_view_id: DocumentViewId,
) -> Result<Option<Task<TaskInput>>, TaskError> {
    debug!("Get view for pinned relation with id: {}", document_view_id);
    match context
        .store
        .get_document_view_by_id(&document_view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
    {
        Some(_) => {
            debug!("View found for pinned relation: {}", document_view_id);
            Ok(None)
        }
        None => {
            debug!("No view found for pinned relation: {}", document_view_id);
            Ok(Some(Task::new(
                "reduce",
                TaskInput::new(None, Some(document_view_id)),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{
        AsVerifiedOperation, OperationId, OperationValue, PinnedRelation, PinnedRelationList,
        Relation, RelationList,
    };
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{
        create_operation, operation_fields, random_document_id, random_document_view_id,
        random_operation_id, verified_operation,
    };
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{
        insert_entry_operation_and_view, test_db, TestDatabase, TestDatabaseRunner,
    };
    use crate::db::traits::DocumentStore;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::{Task, TaskInput};
    use crate::schema::SchemaProvider;

    use super::dependency_task;

    #[rstest]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
            vec![]
        ),
        0
    )]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("favorite_book_images", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ],
            vec![]
        ),
        0
    )]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("something_from_the_past", OperationValue::PinnedRelation(
                    PinnedRelation::new(random_document_view_id())))
            ],
            vec![]
        ),
        1
    )]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("many_previous_drafts", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect())))
            ],
            vec![]
        ),
        2
    )]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ],
            vec![]
        ),
        2
    )]
    // This document has been updated
    #[case(
        test_db(
            4,
            1,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ],
            vec![("one_relation_field", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 3].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                    RelationList::new(
                        [0; 10].iter().map(|_|random_document_id()).collect())))
            ],
        ),
        3
    )]
    fn dispatches_reduce_tasks_for_pinned_child_dependencies(
        #[case] runner: TestDatabaseRunner,
        #[case] expected_next_tasks: usize,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            for document_id in &db.test_data.documents {
                let input = TaskInput::new(Some(document_id.clone()), None);
                reduce_task(context.clone(), input).await.unwrap().unwrap();
            }

            for document_id in &db.test_data.documents {
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
                    assert_eq!(task.worker_name(), "reduce")
                }
            }
        });
    }

    #[rstest]
    fn no_reduce_task_for_materialised_document_relations(
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let document_id = db.test_data.documents[0].clone();

            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(context.clone(), input).await.unwrap().unwrap();

            // Here we have one materialised document, (we are calling it a child as we will
            // shortly be publishing parents) it contains relations which are not materialised yet
            // so should dispatch a reduce task for each one.
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
            assert_eq!(tasks[0].worker_name(), "reduce");
        });
    }

    #[rstest]
    #[case(None, Some(random_document_view_id()))]
    #[case(None, None)]
    #[case(Some(random_document_id()), None)]
    #[case(Some(random_document_id()), Some(random_document_view_id()))]
    fn fails_correctly(
        #[case] document_id: Option<DocumentId>,
        #[case] document_view_id: Option<DocumentViewId>,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store,
                Configuration::default(),
                SchemaProvider::default(),
            );
            let input = TaskInput::new(document_id, document_view_id);
            let next_tasks = dependency_task(context.clone(), input).await;
            assert!(next_tasks.is_err())
        });
    }

    #[rstest]
    #[case(
        test_db(
            2,
            1,
            1,
            true,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("profile_picture", OperationValue::Relation(
                        Relation::new(random_document_id())))
            ],
            vec![]
        )
    )]
    #[case(
        test_db(
            2,
            1,
            1,
            true,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                     PinnedRelationList::new(
                         [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                     RelationList::new(
                         [0; 6].iter().map(|_|random_document_id()).collect())))
            ],
            vec![]
        )
    )]
    fn fails_on_deleted_documents(#[case] runner: TestDatabaseRunner) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let document_id = db.test_data.documents[0].clone();

            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(context.clone(), input).await.unwrap();

            let document_operations = db
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            let document_view_id: DocumentViewId =
                document_operations[1].operation_id().clone().into();

            let input = TaskInput::new(None, Some(document_view_id.clone()));

            let result = dependency_task(context.clone(), input).await;

            assert!(result.is_err())
        });
    }

    // Helper that creates a schema field definition view in the store.
    async fn insert_schema_field_definition_view(
        db: &TestDatabase,
        context: &Context,
    ) -> OperationId {
        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();
        let fields = operation_fields(vec![
            ("name", OperationValue::Text("field_name".to_string())),
            ("type", FieldType::String.into()),
        ]);
        let operation_id = random_operation_id();
        let op = verified_operation(
            Some(fields),
            None,
            Some(SchemaId::SchemaFieldDefinition(1)),
            Some(author),
            Some(operation_id.clone()),
        );
        let document_id = DocumentId::new(operation_id.clone());
        let document_view_id = DocumentViewId::from(operation_id.clone());

        db.store.insert_operation(&op, &document_id).await.unwrap();

        let input = TaskInput::new(None, Some(document_view_id));
        reduce_task(context.clone(), input.clone()).await.unwrap();
        operation_id
    }

    // Helper that creates a schema definition view in the store.
    async fn insert_schema_definition_view(
        field_view_ids: Vec<DocumentViewId>,
        db: &TestDatabase,
        context: &Context,
    ) -> OperationId {
        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();
        let fields = operation_fields(vec![
            ("name", OperationValue::Text("schema_name".to_string())),
            (
                "description",
                OperationValue::Text("description".to_string()),
            ),
            (
                "fields",
                OperationValue::PinnedRelationList(PinnedRelationList::new(field_view_ids)),
            ),
        ]);
        let operation_id = random_operation_id();
        let op = verified_operation(
            Some(fields),
            None,
            Some(SchemaId::SchemaDefinition(1)),
            Some(author),
            Some(operation_id.clone()),
        );
        let document_id = DocumentId::new(operation_id.clone());
        let document_view_id = DocumentViewId::from(operation_id.clone());

        db.store.insert_operation(&op, &document_id).await.unwrap();

        let input = TaskInput::new(None, Some(document_view_id));
        reduce_task(context.clone(), input.clone()).await.unwrap();
        operation_id
    }

    #[rstest]
    fn dispatches_schema_tasks_for_field_definitions(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            // Inserting a schema field definition, we expect a schema task because schema field
            // definitions have no dependencies - every new completed field definition could be the
            // last puzzle piece for a new schema.
            let operation_id = insert_schema_field_definition_view(&db, &context).await;

            let document_view_id = DocumentViewId::from(operation_id);
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks: Vec<&Task<TaskInput>> = tasks
                .iter()
                .filter(|t| t.worker_name() == "schema")
                .collect();
            assert_eq!(schema_tasks.len(), 1);
        });
    }

    #[rstest]
    fn dispatches_schema_tasks_for_schema_definitions(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            // Inserting a schema definition with an unmet dependency we don't expect a schema task.
            let operation_id = insert_schema_definition_view(
                vec![DocumentViewId::from(random_operation_id())],
                &db,
                &context,
            )
            .await;

            let document_view_id = DocumentViewId::from(operation_id);
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks: Vec<&Task<TaskInput>> = tasks
                .iter()
                .filter(|t| t.worker_name() == "schema")
                .collect();
            assert_eq!(schema_tasks.len(), 0);

            // Inserting a schema definition when all schema field definitions are materialised, we
            // expect one schema task.
            let field_definition_view: DocumentViewId =
                insert_schema_field_definition_view(&db, &context)
                    .await
                    .into();
            let operation_id =
                insert_schema_definition_view(vec![field_definition_view], &db, &context).await;

            let document_view_id = DocumentViewId::from(operation_id);
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks: Vec<&Task<TaskInput>> = tasks
                .iter()
                .filter(|t| t.worker_name() == "schema")
                .collect();
            assert_eq!(schema_tasks.len(), 1);
        });
    }
}
