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
    debug!("Working on {}", input);

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
            p2panda_rs::operation::OperationValue::Relation(_) => {
                // This is a relation to a document, if it doesn't exist in the db yet, then that
                // means we either have no entries for this document, or we are not materialising
                // it for some reason. We don't want to kick of a "reduce" or "dependency" task in
                // either of these cases.
                debug!("Relation field found, no action required.");
            }
            p2panda_rs::operation::OperationValue::RelationList(_) => {
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
    let all_dependencies_met = !next_tasks.iter().any(|task| task.is_some());
    if all_dependencies_met {
        let task_input_schema = context
            .store
            .get_schema_by_document_view(document_view.id())
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?
            .ok_or_else(|| {
                TaskError::Failure(format!(
                    "{} was deleted while processing task",
                    document_view
                ))
            })?;

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

    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        AsVerifiedOperation, Operation, OperationValue, PinnedRelation, PinnedRelationList,
        Relation, RelationList,
    };
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::test_utils::constants::SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{
        create_operation, operation_fields, random_document_id, random_document_view_id,
    };
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{
        insert_entry_operation_and_view, send_to_store, test_db, TestDatabase, TestDatabaseRunner,
    };
    use crate::db::traits::DocumentStore;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::schema::SchemaProvider;

    use super::dependency_task;

    #[rstest]
    #[case(
        test_db(
            1,
            1,
            1,
            false,
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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
            SCHEMA_ID.parse().unwrap(),
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

    #[rstest]
    fn dispatches_schema_tasks_for_field_definitions(
        #[from(test_db)]
        #[with(1, 1, 1, false, SchemaId::SchemaFieldDefinition(1), vec![
            ("name", OperationValue::Text("field_name".to_string())),
            ("type", FieldType::String.into()),
        ])]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            // The document id for a schema_field_definition who's operation already exists in the
            // store.
            let document_id = db.test_data.documents.first().unwrap();
            // Materialise the schema field definition.
            let input = TaskInput::new(Some(document_id.to_owned()), None);
            reduce_task(context.clone(), input.clone()).await.unwrap();

            // Parse the document_id into a document_view_id.
            let document_view_id = document_id.as_str().parse().unwrap();
            // Dispatch a dependency task for this document_view_id.
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            // Inserting a schema field definition, we expect a schema task because schema field
            // definitions have no dependencies - every new completed field definition could be the
            // last puzzle piece for a new schema.
            let schema_tasks = tasks.iter().filter(|t| t.worker_name() == "schema").count();

            assert_eq!(schema_tasks, 1);
        });
    }

    #[rstest]
    #[case::schema_definition_with_dependencies_met_dispatches_one_schema_task(
        Operation::new_create(
            SchemaId::SchemaDefinition(1),
            operation_fields(vec![
                ("name", OperationValue::Text("schema_name".to_string())),
                (
                    "description",
                    OperationValue::Text("description".to_string()),
                ),
                (
                    "fields",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                        // The view_id for a schema field which exists in the database. Can be
                        // recreated from variable `schema_field_document_id` in the task below.
                        "0020a2bc0748f3b18627a6aae09ae392561c357a2995528373e730a4d90b73d8c072"
                            .parse().unwrap(),
                    ])),
                ),
            ]),
        ).unwrap(),
        1
    )]
    #[case::schema_definition_without_dependencies_met_dispatches_zero_schema_task(
        Operation::new_create(
            SchemaId::SchemaDefinition(1),
            operation_fields(vec![
                ("name", OperationValue::Text("schema_name".to_string())),
                (
                    "description",
                    OperationValue::Text("description".to_string()),
                ),
                (
                    "fields",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                        // This schema field does not exist in the database
                        random_document_view_id(),
                    ])),
                ),
            ]),
        ).unwrap(),
        0
    )]
    fn dispatches_schema_tasks_for_schema_definitions(
        #[case] schema_create_operation: Operation,
        #[case] expected_schema_tasks: usize,
        #[from(test_db)]
        #[with(1, 1, 1, false, SchemaId::SchemaFieldDefinition(1), vec![
            ("name", OperationValue::Text("field_name".to_string())),
            ("type", FieldType::String.into()),
        ])]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );

            // The document id for the schema_field_definition who's operation already exists in
            // the store.
            let schema_field_document_id = db.test_data.documents.first().unwrap();

            // Materialise the schema field definition.
            let input = TaskInput::new(Some(schema_field_document_id.to_owned()), None);
            reduce_task(context.clone(), input.clone()).await.unwrap();

            // Persist a schema definition entry and operation to the store.
            let (entry_signed, _) =
                send_to_store(&db.store, &schema_create_operation, None, &KeyPair::new()).await;

            // Materialise the schema definition.
            let document_view_id: DocumentViewId = entry_signed.hash().into();
            let input = TaskInput::new(None, Some(document_view_id.clone()));
            reduce_task(context.clone(), input.clone()).await.unwrap();

            // Dispatch a dependency task for the schema definition.
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks = tasks.iter().filter(|t| t.worker_name() == "schema").count();
            assert_eq!(schema_tasks, expected_schema_tasks);
        });
    }
}
