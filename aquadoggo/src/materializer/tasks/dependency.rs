// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::context::Context;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A dependency task prepares _reduce_ tasks for all pinned relations of a given document view.
///
/// The `input` argument must contain only a view id.
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

    // Here we retrieve the document by document view id.
    let document = match &input.document_view_id {
        Some(view_id) => context
            .store
            .get_document_by_view_id(view_id)
            .await
            .map_err(|err| {
                TaskError::Critical(err.to_string())
            })
            ,
        // We expect to handle document_view_ids in a dependency task.
        None => Err(TaskError::Critical("Missing document_view_id in task input".into())),
    }?;

    let document = match document {
        Some(document) => {
            debug!(
                "Document retrieved from storage with view id: {}",
                document.view_id()
            );
            Ok(document)
        }
        // If no document with the view for the id passed into this task could be retrieved then
        // this document has been deleted or the document view does not exist. As "dependency"
        // tasks are only dispatched after a successful "reduce" task, neither `None` case should
        // happen, so this is a critical error.
        None => Err(TaskError::Critical(format!(
            "Expected document with view {} not found in store",
            &input.document_view_id.unwrap()
        ))),
    }?;

    // We can unwrap the view here as only documents with views (meaning they are not deleted) are
    // returned from the store method above.
    let document_view = document.view().unwrap();

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
        // Helper that returns a schema task for the current task input.
        let schema_task = || {
            Some(Task::new(
                "schema",
                TaskInput::new(None, Some(document_view.id().clone())),
            ))
        };

        match document.schema_id() {
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
        .get_document_by_view_id(&document_view_id)
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
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        Operation, OperationBuilder, OperationId, OperationValue, PinnedRelation,
        PinnedRelationList, Relation, RelationList,
    };
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
    use p2panda_rs::test_utils::fixtures::{
        key_pair, random_document_id, random_document_view_id,
    };
    use p2panda_rs::test_utils::memory_store::helpers::{
        populate_store, send_to_store, PopulateStoreConfig,
    };
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::next::{
        add_document, add_schema, doggo_schema, populate_store_config, schema_from_fields,
        test_runner, TestNode,
    };

    use super::dependency_task;

    #[rstest]
    #[case(
        populate_store_config(
            1,
            1,
            1,
            false,
            schema_from_fields(vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))]),
            vec![("profile_picture", OperationValue::Relation(Relation::new(random_document_id())))],
            vec![]
        ),
        0
    )]
    #[case(
        populate_store_config(
            1,
            1,
            1,
            false,
            schema_from_fields(vec![
                ("favorite_book_images", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ]),
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
        populate_store_config(
            1,
            1,
            1,
            false,
            schema_from_fields(vec![
                ("something_from_the_past", OperationValue::PinnedRelation(
                    PinnedRelation::new(random_document_view_id())))
            ]),
            vec![
                ("something_from_the_past", OperationValue::PinnedRelation(
                    PinnedRelation::new(random_document_view_id())))
            ],
            vec![]
        ),
        1
    )]
    #[case(
        populate_store_config(
            1,
            1,
            1,
            false,
            schema_from_fields(vec![
                ("many_previous_drafts", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect())))
            ]),
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
        populate_store_config(
            1,
            1,
            1,
            false,
            schema_from_fields(vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ]),
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
        populate_store_config(
            4,
            1,
            1,
            false,
            schema_from_fields(vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                    PinnedRelationList::new(
                        [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                    RelationList::new(
                        [0; 6].iter().map(|_|random_document_id()).collect())))
            ]),
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
        #[case] config: PopulateStoreConfig,
        #[case] expected_next_tasks: usize,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;

            for document_id in &document_ids {
                let input = TaskInput::new(Some(document_id.clone()), None);
                reduce_task(node.context.clone(), input)
                    .await
                    .unwrap()
                    .unwrap();
            }

            for document_id in &document_ids {
                let document = node
                    .context
                    .store
                    .get_document(document_id)
                    .await
                    .unwrap()
                    .unwrap();

                let input = TaskInput::new(None, Some(document.view_id().clone()));

                let reduce_tasks = dependency_task(node.context.clone(), input)
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
        key_pair: KeyPair,
        #[from(populate_store_config)]
        #[with(2, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            // Here we have one materialised document, (we are calling it a child as we will
            // shortly be publishing parents) it contains relations which are not materialised yet
            // so should dispatch a reduce task for each one.
            let child_document = node
                .context
                .store
                .get_document(&document_id)
                .await
                .unwrap()
                .unwrap();

            let document_view_id_of_child = child_document.view_id();

            let schema = add_schema(
                &mut node,
                "test_schema",
                vec![
                    (
                        "pinned_relation_to_existing_document",
                        FieldType::PinnedRelation(doggo_schema().id().to_owned()),
                    ),
                    (
                        "pinned_relation_to_not_existing_document",
                        FieldType::PinnedRelation(doggo_schema().id().to_owned()),
                    ),
                ],
                &key_pair,
            )
            .await;

            let document_view_id = add_document(
                &mut node,
                schema.id(),
                vec![
                    (
                        "pinned_relation_to_existing_document",
                        OperationValue::PinnedRelation(PinnedRelation::new(
                            document_view_id_of_child.clone(),
                        )),
                    ),
                    (
                        "pinned_relation_to_not_existing_document",
                        OperationValue::PinnedRelation(PinnedRelation::new(
                            random_document_view_id(),
                        )),
                    ),
                ],
                &key_pair,
            )
            .await;

            // The new document should now dispatch one dependency task for the child relation which
            // has not been materialised yet.
            let input = TaskInput::new(None, Some(document_view_id.clone()));
            let tasks = dependency_task(node.context.clone(), input)
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
    ) {
        test_runner(|node: TestNode| async move {
            let input = TaskInput::new(document_id, document_view_id);
            let next_tasks = dependency_task(node.context.clone(), input).await;
            assert!(next_tasks.is_err())
        });
    }

    #[rstest]
    #[case(
        populate_store_config(
            2,
            1,
            1,
            true,
            schema_from_fields(vec![
                ("profile_picture", OperationValue::Relation(
                        Relation::new(random_document_id())))
            ]),
            vec![
                ("profile_picture", OperationValue::Relation(
                        Relation::new(random_document_id())))
            ],
            vec![]
        )
    )]
    #[case(
        populate_store_config(
            2,
            1,
            1,
            true,
            schema_from_fields(vec![
                ("one_relation_field", OperationValue::PinnedRelationList(
                     PinnedRelationList::new(
                         [0; 2].iter().map(|_|random_document_view_id()).collect()))),
                ("another_relation_field", OperationValue::RelationList(
                     RelationList::new(
                         [0; 6].iter().map(|_|random_document_id()).collect())))
            ]),
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
    fn fails_on_deleted_documents(#[case] config: PopulateStoreConfig) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(node.context.clone(), input).await.unwrap();

            let document_operations = node
                .context
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            let document_view_id: DocumentViewId =
                WithId::<OperationId>::id(&document_operations[1])
                    .clone()
                    .into();

            let input = TaskInput::new(None, Some(document_view_id.clone()));

            let result = dependency_task(node.context.clone(), input).await;

            assert!(result.is_err())
        });
    }

    #[rstest]
    fn dispatches_schema_tasks_for_field_definitions(
        #[from(populate_store_config)]
        #[with(1, 1, 1, false, Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap().to_owned(), vec![
            ("name", OperationValue::String("field_name".to_string())),
            ("type", FieldType::String.into()),
        ])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            // Materialise the schema field definition.
            let input = TaskInput::new(Some(document_id.to_owned()), None);
            reduce_task(node.context.clone(), input.clone())
                .await
                .unwrap();

            // Parse the document_id into a document_view_id.
            let document_view_id = document_id.as_str().parse().unwrap();
            // Dispatch a dependency task for this document_view_id.
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(node.context.clone(), input)
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
        OperationBuilder::new(&SchemaId::SchemaDefinition(1))
            .fields(&[
                ("name", OperationValue::String("schema_name".to_string())),
                (
                    "description",
                    OperationValue::String("description".to_string()),
                ),
                (
                    "fields",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                        // The view_id for a schema field which exists in the database. Can be
                        // recreated from variable `schema_field_document_id` in the task below.
                        "0020a72d9fbe9c9a8e27825afba535d76340d0cb45bf75d706f4ec6299bfd6a0bc2e"
                            .parse().unwrap(),
                    ])),
                ),
            ]).build().unwrap(),
        1
    )]
    #[case::schema_definition_without_dependencies_met_dispatches_zero_schema_task(
        OperationBuilder::new(&SchemaId::SchemaDefinition(1))
            .fields(&[
                ("name", OperationValue::String("schema_name".to_string())),
                (
                    "description",
                    OperationValue::String("description".to_string()),
                ),
                (
                    "fields",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                        // This schema field does not exist in the database
                        random_document_view_id(),
                    ])),
                ),
            ]).build().unwrap(),
        0
    )]
    fn dispatches_schema_tasks_for_schema_definitions(
        #[case] schema_create_operation: Operation,
        #[case] expected_schema_tasks: usize,
        #[from(populate_store_config)]
        #[with(1, 1, 1, false, Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap().to_owned(), vec![
            ("name", OperationValue::String("field_name".to_string())),
            ("type", FieldType::String.into()),
        ])]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let schema_field_document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            // Materialise the schema field definition.
            let input = TaskInput::new(Some(schema_field_document_id.to_owned()), None);
            reduce_task(node.context.clone(), input.clone())
                .await
                .unwrap();

            // Persist a schema definition entry and operation to the store.
            let (entry_signed, _) = send_to_store(
                &node.context.store,
                &schema_create_operation,
                Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
                &KeyPair::new(),
            )
            .await
            .unwrap();

            // Materialise the schema definition.
            let document_id: DocumentId = entry_signed.hash().into();
            let input = TaskInput::new(Some(document_id.clone()), None);
            reduce_task(node.context.clone(), input.clone())
                .await
                .unwrap();

            // Dispatch a dependency task for the schema definition.
            let document_view_id: DocumentViewId = entry_signed.hash().into();
            let input = TaskInput::new(None, Some(document_view_id));
            let tasks = dependency_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks = tasks.iter().filter(|t| t.worker_name() == "schema").count();
            assert_eq!(schema_tasks, expected_schema_tasks);
        });
    }
}
