// SPDX-License-Identifier: AGPL-3.0-or-later

use log::{debug, trace};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, SchemaId};
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
/// already, but they were never materialised to the document view required by the pinned relation.
/// In order to guarantee all required document views are present we dispatch a reduce task for the
/// view of each pinned relation found.
///
/// Expects a _reduce_ task to have completed successfully for the given document view itself and
/// returns a critical error otherwise.
pub async fn dependency_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let mut is_current_view = false;

    let view_id = match input {
        TaskInput::SpecificView(view_id) => view_id,
        TaskInput::CurrentView(view_id) => {
            is_current_view = true;
            view_id
        }
        _ => {
            return Err(TaskError::Critical(
                "Missing document view id in task input".into(),
            ))
        }
    };

    let document = context
        .store
        .get_document_by_view_id(&view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    let document = match document {
        Some(document) => {
            trace!("Document retrieved from storage with view id: {}", view_id);
            Ok(document)
        }
        // If no document with the view for the id passed into this task could be retrieved then
        // this document has been deleted or the document view does not exist. As "dependency"
        // tasks are only dispatched after a successful "reduce" task this is fairly rare, usually
        // a result of a race condition where other tasks changed the state before the "dependency"
        // task got dispatched.
        None => Err(TaskError::Failure(format!(
            "Expected document with view {} not found in store",
            view_id
        ))),
    }?;

    // We can unwrap the view here as only documents with views (meaning they are not deleted) are
    // returned from the store method above.
    let document_view = document.view().unwrap();

    let mut next_tasks = Vec::new();

    // First we handle all pinned or unpinned relations defined in this document view. We can think
    // of these as "child" relations.
    for (_field_name, document_view_value) in document_view.fields().iter() {
        match document_view_value.value() {
            OperationValue::Relation(_) => {
                // This is a relation to a document, if it doesn't exist in the db yet, then that
                // means we either have no entries for this document, or we are not materialising
                // it for some reason. We don't want to kick of a "reduce" or "dependency" task in
                // either of these cases.
                trace!("Relation field found, no action required.");
            }
            OperationValue::RelationList(_) => {
                // same as above...
                trace!("Relation list field found, no action required.");
            }
            OperationValue::PinnedRelation(pinned_relation) => {
                // These are pinned relations. We may have the operations for these views in the
                // db, but this view wasn't pinned yet, so hasn't been materialised. To make sure
                // it is materialised when possible, we dispatch a "reduce" task for any pinned
                // relations which aren't found.
                trace!(
                    "Pinned relation field found referring to view id: {}",
                    pinned_relation.view_id()
                );

                if let Some(task) =
                    get_relation_task(&context, pinned_relation.view_id().clone()).await?
                {
                    next_tasks.push(task);
                }
            }
            OperationValue::PinnedRelationList(pinned_relation_list) => {
                // same as above...
                if pinned_relation_list.len() == 0 {
                    trace!("Pinned relation list field containing no items");
                } else {
                    for document_view_id in pinned_relation_list.iter() {
                        trace!(
                            "Pinned relation list field found containing view id: {}",
                            document_view_id
                        );

                        if let Some(task) =
                            get_relation_task(&context, document_view_id.clone()).await?
                        {
                            next_tasks.push(task);
                        }
                    }
                }
            }
            _ => (),
        }
    }

    // Construct additional tasks if the task input matches certain system schemas and all
    // "child" dependencies have been reduced
    let child_dependencies_met = next_tasks.is_empty();
    if child_dependencies_met {
        match document.schema_id() {
            // Start `schema` task when a schema (field) definition view is completed with
            // dependencies
            SchemaId::SchemaDefinition(_) | SchemaId::SchemaFieldDefinition(_) => {
                next_tasks.push(Task::new(
                    "schema",
                    TaskInput::SpecificView(document_view.id().clone()),
                ));
            }
            SchemaId::Blob(_) | SchemaId::BlobPiece(_) => {
                let input = if is_current_view {
                    TaskInput::CurrentView(document_view.id().clone())
                } else {
                    TaskInput::SpecificView(document_view.id().clone())
                };
                next_tasks.push(Task::new("blob", input));
            }
            _ => {}
        }
    }

    // Now we check all the "parent" or "inverse" relations, that is _other_ documents pointing at
    // the one we're currently looking at
    let mut reverse_tasks = get_inverse_relation_tasks(&context, document.schema_id()).await?;
    next_tasks.append(&mut reverse_tasks);

    debug!("Scheduling {} tasks", next_tasks.len());

    Ok(Some(next_tasks))
}

/// Returns a _reduce_ task for a given document view only if that view does not yet exist in the
/// store.
async fn get_relation_task(
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
                TaskInput::SpecificView(document_view_id),
            )))
        }
    }
}

/// Returns _dependency_ tasks for every document which has a pinned relation or pinned relation
/// list to a document view with the given schema id.
async fn get_inverse_relation_tasks(
    context: &Context,
    schema_id: &SchemaId,
) -> Result<Vec<Task<TaskInput>>, TaskError> {
    let mut tasks = Vec::new();

    // Find all "parent" schemas which have at least one relation field pointing at documents of
    // the given schema id
    let parent_schema_ids: Vec<SchemaId> = context
        .schema_provider
        .all()
        .await
        .iter()
        .filter_map(|schema| {
            let has_relation_to_schema =
                schema
                    .fields()
                    .iter()
                    .any(|(_, field_type)| match field_type {
                        FieldType::PinnedRelation(relation_schema_id)
                        | FieldType::PinnedRelationList(relation_schema_id) => {
                            relation_schema_id == schema_id
                        }
                        _ => false,
                    });

            if has_relation_to_schema {
                Some(schema.id().to_owned())
            } else {
                None
            }
        })
        .collect();

    // Find all documents which follow these "parent" schemas
    for parent_schema_id in parent_schema_ids {
        // @TODO: Use a more efficient SQL query here, this does too much
        let parent_documents = context
            .store
            .get_documents_by_schema(&parent_schema_id)
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?;

        for parent_document in parent_documents {
            // Dispatch dependency tasks from the latest document view of each parent document. We
            // _only_ materialise historical document views when at least one _latest_ document
            // view points at it.
            tasks.push(Task::new(
                "dependency",
                TaskInput::SpecificView(parent_document.view_id().to_owned()),
            ));
        }
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::decode::decode_entry;
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::encode::encode_operation;
    use p2panda_rs::operation::{
        Operation, OperationAction, OperationBuilder, OperationId, OperationValue, PinnedRelation,
        PinnedRelationList, Relation, RelationList,
    };
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::storage_provider::traits::{DocumentStore, EntryStore, OperationStore};
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_id, random_document_view_id};
    use p2panda_rs::test_utils::memory_store::helpers::{
        populate_store, send_to_store, PopulateStoreConfig,
    };
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::{Task, TaskInput};
    use crate::test_utils::{
        add_document, add_schema, doggo_schema, populate_store_config, schema_from_fields,
        test_runner, test_runner_with_manager, TestNode, TestNodeManager,
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
                let input = TaskInput::DocumentId(document_id.clone());
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

                let input = TaskInput::SpecificView(document.view_id().clone());

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

            let input = TaskInput::DocumentId(document_id.clone());
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
            let input = TaskInput::SpecificView(document_view_id.clone());
            let tasks = dependency_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].worker_name(), "reduce");
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

            let input = TaskInput::DocumentId(document_id.clone());
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

            let input = TaskInput::SpecificView(document_view_id.clone());

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
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            // Materialise the schema field definition.
            let input = TaskInput::DocumentId(document_id.to_owned());
            reduce_task(node.context.clone(), input.clone())
                .await
                .unwrap();

            // Parse the document_id into a document_view_id.
            let document_view_id = document_id.as_str().parse().unwrap();

            // Dispatch a dependency task for this document_view_id.
            let input = TaskInput::SpecificView(document_view_id);
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
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let schema_field_document_id = document_ids
                .get(0)
                .expect("Should be at least one document id");

            // Materialise the schema field definition.
            let input = TaskInput::DocumentId(schema_field_document_id.to_owned());
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
            let input = TaskInput::DocumentId(document_id.clone());
            reduce_task(node.context.clone(), input.clone())
                .await
                .unwrap();

            // Dispatch a dependency task for the schema definition.
            let document_view_id: DocumentViewId = entry_signed.hash().into();
            let input = TaskInput::SpecificView(document_view_id);
            let tasks = dependency_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            let schema_tasks = tasks.iter().filter(|t| t.worker_name() == "schema").count();
            assert_eq!(schema_tasks, expected_schema_tasks);
        });
    }

    #[rstest]
    fn materialise_late_views(key_pair: KeyPair) {
        test_runner_with_manager(|manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let node_b = manager.create().await;

            // PREPARE TEST DATA

            // Create a post schema with comments pinned to a particular version of each post
            let post_schema = add_schema(
                &mut node_a,
                "post",
                vec![("title", FieldType::String)],
                &key_pair,
            )
            .await;

            let comment_schema = add_schema(
                &mut node_a,
                "comment",
                vec![
                    ("post", FieldType::PinnedRelation(post_schema.id().clone())),
                    ("text", FieldType::String),
                ],
                &key_pair,
            )
            .await;

            // 1. Create document with cooking recipe
            let post_operation_1 = OperationBuilder::new(post_schema.id())
                .fields(&[("title", "How to make potato soup".into())])
                .build()
                .unwrap();

            let (post_entry_1, _) = send_to_store(
                &node_a.context.store,
                &post_operation_1,
                &post_schema,
                &key_pair,
            )
            .await
            .unwrap();

            let post_operation_id_1 = post_entry_1.hash().into();
            let post_document_id = DocumentId::new(&post_operation_id_1);
            let post_view_id_1 = DocumentViewId::new(&[post_operation_id_1.clone()]);

            // 2. Update it (later this will be the version someone will write a comment about)
            let post_operation_2 = OperationBuilder::new(post_schema.id())
                .action(OperationAction::Update)
                .previous(&post_view_id_1)
                .fields(&[(
                    "title",
                    "How to make potato soup, definitive edition!".into(),
                )])
                .build()
                .unwrap();

            let (post_entry_2, _) = send_to_store(
                &node_a.context.store,
                &post_operation_2,
                &post_schema,
                &key_pair,
            )
            .await
            .unwrap();

            let post_operation_id_2: OperationId = post_entry_2.hash().into();
            let post_view_id_2 = DocumentViewId::new(&[post_operation_id_2.clone()]);

            // 3. Update it, so there is another version after the one someone was commenting on
            let post_operation_3 = OperationBuilder::new(post_schema.id())
                .action(OperationAction::Update)
                .previous(&post_view_id_2)
                .fields(&[(
                    "title",
                    "How to make the ULTIMATE potato soup! Now for real, with bamboo grass.".into(),
                )])
                .build()
                .unwrap();

            let (post_entry_3, _) = send_to_store(
                &node_a.context.store,
                &post_operation_3,
                &post_schema,
                &key_pair,
            )
            .await
            .unwrap();

            let post_operation_id_3: OperationId = post_entry_3.hash().into();
            let post_view_id_3 = DocumentViewId::new(&[post_operation_id_3.clone()]);

            // 4. Write the comment on the recipe post, when it was in its second version
            let comment_operation_1 = OperationBuilder::new(comment_schema.id())
                .fields(&[(
                    "text",
                    "Sorry, but I think my potato soup is much better. Did you try bamboo grass?"
                        .into(),
                ), (
                    "post",
                    post_view_id_2.clone().into(),
                )])
                .build()
                .unwrap();

            let (comment_entry_1, _) = send_to_store(
                &node_a.context.store,
                &comment_operation_1,
                &comment_schema,
                &key_pair,
            )
            .await
            .unwrap();

            let comment_operation_id_1 = comment_entry_1.hash().into();
            let comment_document_id = DocumentId::new(&comment_operation_id_1);
            let comment_view_id_1 = DocumentViewId::new(&[comment_operation_id_1.clone()]);

            // INSERT SCHEMAS INTO NODE B

            let _ = node_b.context.schema_provider.update(post_schema).await;
            let _ = node_b.context.schema_provider.update(comment_schema).await;

            // INSERT "COMMENT" INTO NODE B'S DATABASE

            // We do this to enforce an ordering of when the system processes what information. In
            // reality that ordering might look different / be random.

            node_b
                .context
                .store
                .insert_operation(
                    &comment_operation_id_1,
                    &key_pair.public_key(),
                    &comment_operation_1,
                    &comment_document_id,
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_entry(
                    &decode_entry(&comment_entry_1).unwrap(),
                    &comment_entry_1,
                    Some(&encode_operation(&comment_operation_1).unwrap()),
                )
                .await
                .unwrap();

            // 1. Materialise the "comment" document first, it will point at a post which does not
            //    exist yet ..
            let input = TaskInput::DocumentId(comment_document_id);
            let tasks = reduce_task(node_b.context.clone(), input)
                .await
                .unwrap()
                .expect("Should have returned new tasks");
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].worker_name(), &String::from("dependency"));

            // 2. The "dependency" task will try to resolve the pinned document view pointing at
            //    the "post" document in it's version 2
            let tasks = dependency_task(node_b.context.clone(), tasks[0].input().clone())
                .await
                .unwrap();
            assert_eq!(
                tasks,
                Some(vec![Task::new(
                    "reduce",
                    TaskInput::SpecificView(post_view_id_2.clone())
                )])
            );

            // 3. .. this kicks in a "reduce" task for that particular view, but it will cancel
            //    since not enough data exists to materialize the "post" document
            let tasks = reduce_task(
                node_b.context.clone(),
                TaskInput::SpecificView(post_view_id_2.clone()),
            )
            .await
            .unwrap();
            assert!(tasks.is_none());

            // INSERT "POST" INTO NODE B'S DATABASE

            node_b
                .context
                .store
                .insert_operation(
                    &post_operation_id_1,
                    &key_pair.public_key(),
                    &post_operation_1,
                    &post_document_id,
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_entry(
                    &decode_entry(&post_entry_1).unwrap(),
                    &post_entry_1,
                    Some(&encode_operation(&post_operation_1).unwrap()),
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_operation(
                    &post_operation_id_2,
                    &key_pair.public_key(),
                    &post_operation_2,
                    &post_document_id,
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_entry(
                    &decode_entry(&post_entry_2).unwrap(),
                    &post_entry_2,
                    Some(&encode_operation(&post_operation_2).unwrap()),
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_operation(
                    &post_operation_id_3,
                    &key_pair.public_key(),
                    &post_operation_3,
                    &post_document_id,
                )
                .await
                .unwrap();

            node_b
                .context
                .store
                .insert_entry(
                    &decode_entry(&post_entry_3).unwrap(),
                    &post_entry_3,
                    Some(&encode_operation(&post_operation_3).unwrap()),
                )
                .await
                .unwrap();

            // 1. Now materialise the post, it will exist now in its latest form, but the comment
            //    will point at an old version of it, and this historical view does not exist yet
            //    ..
            let input = TaskInput::DocumentId(post_document_id);
            let tasks = reduce_task(node_b.context.clone(), input)
                .await
                .unwrap()
                .expect("Should have returned new tasks");
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].worker_name(), &String::from("dependency"));

            // We should have now a materialized latest post and comment document but not the
            // pinned historical version of the post, where the comment was pointing at!
            assert!(node_b
                .context
                .store
                .get_document_by_view_id(&post_view_id_3)
                .await
                .unwrap()
                .is_some());

            assert!(node_b
                .context
                .store
                .get_document_by_view_id(&comment_view_id_1)
                .await
                .unwrap()
                .is_some());

            assert!(node_b
                .context
                .store
                .get_document_by_view_id(&post_view_id_2)
                .await
                .unwrap()
                .is_none()); // Should not exist yet!

            // 2. The "dependency" task followed materialising the "post" found a reverse relation
            //    to a "comment" document .. it dispatches another "dependency" task for it
            let tasks = dependency_task(node_b.context.clone(), tasks[0].input().clone())
                .await
                .unwrap();
            assert_eq!(
                tasks,
                Some(vec![Task::new(
                    "dependency",
                    TaskInput::SpecificView(comment_view_id_1.clone())
                )])
            );

            // 3. Running the "dependency" task again over the "comment" document we finally get
            //    back to the missing pinned relation to the older version of the "post" document
            let tasks = dependency_task(
                node_b.context.clone(),
                TaskInput::SpecificView(comment_view_id_1.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                tasks,
                Some(vec![Task::new(
                    "reduce",
                    TaskInput::SpecificView(post_view_id_2.clone())
                )])
            );

            // 4. We finally materialise the missing view!
            let tasks = reduce_task(
                node_b.context.clone(),
                TaskInput::SpecificView(post_view_id_2.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                tasks,
                Some(vec![Task::new(
                    "dependency",
                    TaskInput::SpecificView(post_view_id_2.clone())
                )])
            );

            // It exists now! :-)
            assert!(node_b
                .context
                .store
                .get_document_by_view_id(&post_view_id_2)
                .await
                .unwrap()
                .is_some());

            // 5. Running the two dispatched "dependency" tasks again ends with no more tasks. This
            //    is good, otherwise we would run into an infinite, recursive loop
            let tasks = dependency_task(
                node_b.context.clone(),
                TaskInput::SpecificView(post_view_id_2.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                tasks,
                Some(vec![Task::new(
                    "dependency",
                    TaskInput::SpecificView(comment_view_id_1.clone())
                )])
            );

            let tasks = dependency_task(
                node_b.context.clone(),
                TaskInput::SpecificView(comment_view_id_1.clone()),
            )
            .await
            .unwrap();
            assert_eq!(tasks, Some(vec![]));
        });
    }
}
