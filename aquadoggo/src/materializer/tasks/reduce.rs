// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use log::{debug, trace};
use p2panda_rs::document::materialization::build_graph;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{Document, DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::operation::traits::{AsOperation, WithPublicKey};
use p2panda_rs::operation::OperationId;
use p2panda_rs::storage_provider::traits::{DocumentStore, EntryStore, LogStore, OperationStore};
use p2panda_rs::{Human, WithId};

use crate::context::Context;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// Build a materialised view for a document by reducing the document's operation graph and storing
/// it to disk.
///
/// ## Task input
///
/// If the task input contains a document view id, only this particular view is materialised.
///
/// If the task input contains a document id, the latest view for that document is built and stored
/// and also, the document itself is updated in the store.
///
/// ## Integration with other tasks
///
/// A reduce task is dispatched for every entry and operation pair which arrives at a node.
///
/// They may also be dispatched from a dependency task when a pinned relations is present on an
/// already materialised document view.
pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    // Find out which document we are handling
    let document_id = if let Some(document_id) = resolve_document_id(&context, &input).await? {
        document_id
    } else {
        debug!("No document found for this view, exit without dispatching any other tasks");
        return Ok(None);
    };

    // Get all operations for the requested document
    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    match &input {
        TaskInput::DocumentId(_) => reduce_document(&context, &operations).await,
        TaskInput::SpecificView(view_id) | TaskInput::CurrentView(view_id) => {
            reduce_document_view(&context, &document_id, view_id, &operations).await
        }
    }
}

/// Helper method to resolve a document id from a task input.
async fn resolve_document_id<S: EntryStore + OperationStore + LogStore + DocumentStore>(
    context: &Context<S>,
    input: &TaskInput,
) -> Result<Option<DocumentId>, TaskError> {
    match input {
        TaskInput::DocumentId(document_id) => {
            // Id is already given, we don't have to do anything
            Ok(Some(document_id.to_owned()))
        }
        TaskInput::SpecificView(document_view_id) | TaskInput::CurrentView(document_view_id) => {
            // Document view id is given, let's find out its document id
            trace!("Find document for view with id: {}", document_view_id);

            // Pick one operation from the view, this is all we need to determine the document id
            let operation_id = document_view_id.iter().last().unwrap();

            // Determine document id by looking into the operations stored on the node already.
            // Note, finding a document id here does not mean the document has been materialized
            // yet, just that we have the operations waiting. We need to check the document exists
            // in a following step.
            context
                .store
                .get_document_id_by_operation_id(operation_id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))
        }
    }
}

/// Helper method to reduce an operation graph to a specific document view, returning the view id
/// of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document_view<O: AsOperation + WithId<OperationId> + WithPublicKey>(
    context: &Context,
    document_id: &DocumentId,
    document_view_id: &DocumentViewId,
    operations: &Vec<O>,
) -> Result<Option<Vec<Task<TaskInput>>>, TaskError> {
    // Attempt to retrieve the document this view is part of in order to determine if it has
    // been once already materialized yet.
    // @TODO: This can be a more efficient storage method. See issue:
    // https://github.com/p2panda/aquadoggo/issues/431
    let document_exists = context
        .store
        .get_document(document_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .is_some();

    // If it wasn't found then we shouldn't reduce this view yet (as the document it's part of
    // should be reduced first).
    //
    // In this case, we assume the task for reducing the document is triggered in the future which
    // is followed by a dependency task finally looking at all view ids of this document (through
    // pinned relation ids pointing at it).
    if !document_exists {
        return Ok(None);
    };

    // Make sure to not materialize and store document view twice
    // @TODO: This can be a more efficient storage method. See issue:
    // https://github.com/p2panda/aquadoggo/issues/431
    let document_view_exists = context
        .store
        .get_document_by_view_id(document_view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .is_some();

    if document_view_exists {
        return Ok(None);
    }

    // Materialize document view
    let document_builder: DocumentBuilder = operations.into();
    let document = match document_builder.build_to_view_id(Some(document_view_id.to_owned())) {
        Ok(document) => {
            // If the document was deleted, then we return nothing
            debug!(
                "Document materialized to view with id: {}",
                document_view_id
            );

            if document.is_deleted() {
                return Ok(None);
            };

            document
        }
        Err(err) => {
            debug!(
                "Document view materialization failed view with id: {}",
                document_view_id
            );
            debug!("{}", err);

            // There is not enough operations yet to materialise this view. Maybe next time!
            return Ok(None);
        }
    };

    context
        .store
        .insert_document_view(
            &document.view().unwrap(),
            document.id(),
            document.schema_id(),
        )
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    debug!("Stored {} document view {}", document, document.view_id());

    debug!(
        "Dispatch dependency task for view with id: {}",
        document.view_id()
    );

    Ok(Some(vec![Task::new(
        "dependency",
        TaskInput::SpecificView(document.view_id().to_owned()),
    )]))
}

/// Helper method to reduce an operation graph to the latest document view, returning the
/// `DocumentViewId` of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document<O: AsOperation + WithId<OperationId> + WithPublicKey>(
    context: &Context,
    operations: &Vec<O>,
) -> Result<Option<Vec<Task<TaskInput>>>, TaskError> {
    match Document::try_from(operations) {
        Ok(document) => {
            // Make sure to not materialize and store document view twice
            // @TODO: This can be a more efficient storage method. See issue:
            // https://github.com/p2panda/aquadoggo/issues/431
            let document_view_exists = context
                .store
                .get_document_by_view_id(document.view_id())
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?
                .is_some();

            if document_view_exists {
                return Ok(None);
            };

            // @TODO: Make sorted operations available after building the document above so we can skip this step.
            let operations = DocumentBuilder::from(operations).operations();
            let sorted_operations = build_graph(&operations).unwrap().sort().unwrap().sorted();

            // Iterate over the sorted document operations and update their sorted index on the
            // operations_v1 table.
            for (index, (operation_id, _, _)) in sorted_operations.iter().enumerate() {
                context
                    .store
                    .update_operation_index(operation_id, index as i32)
                    .await
                    .map_err(|err| TaskError::Critical(err.to_string()))?;
            }

            // Insert this document into storage. If it already existed, this will update it's
            // current view
            context
                .store
                .insert_document(&document)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // If the document was deleted, then we return nothing
            if document.is_deleted() {
                debug!(
                    "Deleted {} final view {}",
                    document.display(),
                    document.view_id().display()
                );
                return Ok(None);
            }

            if document.is_edited() {
                debug!(
                    "Updated {} latest view {}",
                    document.display(),
                    document.view_id().display()
                );
            } else {
                debug!("Created {}", document.display());
            };

            debug!(
                "Dispatch dependency task for view with id: {}",
                document.view_id()
            );
            Ok(Some(vec![Task::new(
                "dependency",
                TaskInput::CurrentView(document.view_id().to_owned()),
            )]))
        }
        Err(err) => {
            // There is not enough operations yet to materialise this view. Maybe next time!
            debug!("Document materialization error: {}", err);

            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::materialization::build_graph;
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{
        Document, DocumentBuilder, DocumentId, DocumentViewFields, DocumentViewId,
        DocumentViewValue,
    };
    use p2panda_rs::operation::traits::AsOperation;
    use p2panda_rs::operation::OperationValue;
    use p2panda_rs::schema::Schema;
    use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
    use p2panda_rs::test_utils::constants;
    use p2panda_rs::test_utils::fixtures::{
        operation, operation_fields, random_document_id, random_document_view_id, schema,
    };
    use p2panda_rs::test_utils::memory_store::helpers::{
        populate_store, send_to_store, PopulateStoreConfig,
    };
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{
        doggo_fields, doggo_schema, populate_store_config, test_runner, TestNode,
    };

    #[rstest]
    fn reduces_documents(
        #[from(populate_store_config)]
        #[with(
            2,
            1,
            20,
            false,
            doggo_schema(),
            doggo_fields(),
            vec![("username", OperationValue::String("PANDA".into()))]
        )]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;

            for document_id in &document_ids {
                let input = TaskInput::DocumentId(document_id.clone());
                assert!(reduce_task(node.context.clone(), input).await.is_ok());
            }

            for document_id in &document_ids {
                let document = node.context.store.get_document(document_id).await.unwrap();

                assert_eq!(
                    document.unwrap().get("username").unwrap(),
                    &OperationValue::String("PANDA".to_string())
                )
            }
        });
    }

    #[rstest]
    fn updates_a_document(
        schema: Schema,
        #[from(populate_store_config)]
        #[with(
            1,
            1,
            1,
            false,
            constants::schema(),
            constants::test_fields(),
            constants::test_fields()
        )]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (key_pairs, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("There should be at least one document id");

            let key_pair = key_pairs
                .get(0)
                .expect("There should be at least one key_pair");

            let input = TaskInput::DocumentId(document_id.clone());

            // There is one CREATE operation for this document in the db, it should create a document
            // in the documents table.
            assert!(reduce_task(node.context.clone(), input.clone())
                .await
                .is_ok());

            // Now we create and insert an UPDATE operation for this document.
            let (_, _) = send_to_store(
                &node.context.store,
                &operation(
                    Some(operation_fields(vec![(
                        "username",
                        OperationValue::String("meeeeeee".to_string()),
                    )])),
                    Some(document_id.as_str().parse().unwrap()),
                    schema.id().to_owned(),
                ),
                &schema,
                key_pair,
            )
            .await
            .unwrap();

            // This should now find the new UPDATE operation and perform an update on the document
            // in the documents table.
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // The new view should exist and the document should refer to it.
            let document = node.context.store.get_document(document_id).await.unwrap();
            assert_eq!(
                document.unwrap().get("username").unwrap(),
                &OperationValue::String("meeeeeee".to_string())
            )
        })
    }

    #[rstest]
    fn reduces_document_to_specific_view_id(
        #[from(populate_store_config)]
        #[with(
            2,
            1,
            1,
            false,
            doggo_schema(),
            doggo_fields(),
            vec![("username", OperationValue::String("PANDA".into()))]
        )]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("There should be at least one document id");

            // Get the operations
            let document_operations = node
                .context
                .store
                .get_operations_by_document_id(document_id)
                .await
                .unwrap();

            // Sort the operations into their ready for reducing order
            let document_builder = DocumentBuilder::from(&document_operations);
            let sorted_document_operations = build_graph(&document_builder.operations())
                .unwrap()
                .sort()
                .unwrap()
                .sorted();

            // Reduce document to it's current view and insert into database
            let input = TaskInput::DocumentId(document_id.clone());
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // We should be able to query this specific view now and receive the expected state
            let document_view_id: DocumentViewId =
                sorted_document_operations.get(1).unwrap().clone().0.into();
            let document = node
                .context
                .store
                .get_document_by_view_id(&document_view_id)
                .await
                .unwrap();

            assert_eq!(
                document.unwrap().get("username").unwrap(),
                &OperationValue::String("PANDA".to_string())
            );

            // We didn't reduce this document_view so it shouldn't exist in the db.
            let document_view_id: DocumentViewId =
                sorted_document_operations.get(0).unwrap().clone().0.into();

            let document = node
                .context
                .store
                .get_document_by_view_id(&document_view_id)
                .await
                .unwrap();

            assert!(document.is_none());

            // But now if we do request an earlier view is materialised for this document...
            let input = TaskInput::SpecificView(document_view_id.clone());
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // Then we should now be able to query it and revieve the expected value.
            let document = node
                .context
                .store
                .get_document_by_view_id(&document_view_id)
                .await
                .unwrap();

            assert!(document.is_some());
            assert_eq!(
                document.unwrap().get("username").unwrap(),
                &OperationValue::String("bubu".to_string())
            );
        });
    }

    #[rstest]
    fn deleted_documents_have_no_view(
        #[from(populate_store_config)]
        #[with(3, 1, 2, true)]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;

            for document_id in &document_ids {
                let input = TaskInput::DocumentId(document_id.clone());
                let tasks = reduce_task(node.context.clone(), input).await.unwrap();
                assert!(tasks.is_none());
            }

            for document_id in &document_ids {
                let document = node.context.store.get_document(document_id).await.unwrap();
                assert!(document.is_none())
            }

            let document_operations = node
                .context
                .store
                .get_operations_by_document_id(&document_ids[0])
                .await
                .unwrap();

            let document = Document::try_from(&document_operations).unwrap();

            let input = TaskInput::SpecificView(document.view_id().clone());
            let tasks = reduce_task(node.context.clone(), input).await.unwrap();

            assert!(tasks.is_none());
        });
    }

    #[rstest]
    #[case(
        populate_store_config(3, 1, 1, false, doggo_schema(), doggo_fields(), doggo_fields()),
        true
    )]
    // This document is deleted, it shouldn't spawn a dependency task.
    #[case(
        populate_store_config(3, 1, 1, true, doggo_schema(), doggo_fields(), doggo_fields()),
        false
    )]
    fn returns_dependency_task_inputs(
        #[case] config: PopulateStoreConfig,
        #[case] is_next_task: bool,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("There should be at least one document id");

            let input = TaskInput::DocumentId(document_id.clone());
            let next_task_inputs = reduce_task(node.context.clone(), input).await.unwrap();

            assert_eq!(next_task_inputs.is_some(), is_next_task);
        });
    }

    #[rstest]
    fn does_not_error_when_document_missing(
        #[from(random_document_id)] document_id: DocumentId,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
    ) {
        // Prepare empty database.
        test_runner(move |node: TestNode| async move {
            // Dispatch a reduce task for a document which doesn't exist by it's document id.
            let input = TaskInput::DocumentId(document_id);
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // Dispatch a reduce task for a document which doesn't exist by it's document view id.
            let input = TaskInput::SpecificView(document_view_id);
            assert!(reduce_task(node.context.clone(), input).await.is_ok());
        });
    }

    #[rstest]
    fn duplicate_document_view_insertions(
        #[from(populate_store_config)]
        #[with(2, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations and build the document
            let operations = node
                .context
                .store
                .get_operations_by_document_id(document_id)
                .await
                .unwrap();

            // Build the document from the operations
            let document_builder = DocumentBuilder::from(&operations);
            let document = document_builder.build().unwrap();

            // Issue a reduce task for the document, which also inserts the current view
            let input = TaskInput::DocumentId(document_id.clone());
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // Issue a reduce task for the document view, which should succeed although no new view
            // is inserted
            let input = TaskInput::SpecificView(document.view_id().clone());
            assert!(reduce_task(node.context.clone(), input).await.is_ok());
        })
    }

    #[rstest]
    fn updates_operations_sorted_index(
        schema: Schema,
        #[from(populate_store_config)]
        #[with(
            3,
            1,
            1,
            false,
            constants::schema(),
            constants::test_fields(),
            constants::test_fields()
        )]
        config: PopulateStoreConfig,
    ) {
        test_runner(move |node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (key_pairs, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("There should be at least one document id");

            let key_pair = key_pairs
                .get(0)
                .expect("There should be at least one key_pair");

            // Now we create and insert an UPDATE operation for this document which is pointing at
            // the root CREATE operation.
            let (_, _) = send_to_store(
                &node.context.store,
                &operation(
                    Some(operation_fields(vec![(
                        "username",
                        OperationValue::String("hello".to_string()),
                    )])),
                    Some(document_id.as_str().parse().unwrap()),
                    schema.id().to_owned(),
                ),
                &schema,
                key_pair,
            )
            .await
            .unwrap();

            // Before running the reduce task retrieve the operations. These should not be in
            // their topologically sorted order.
            let pre_materialization_operations = node
                .context
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            // Run a reduce task with the document id as input.
            let input = TaskInput::DocumentId(document_id.clone());
            assert!(reduce_task(node.context.clone(), input.clone())
                .await
                .is_ok());

            // Retrieve the operations again, they should now be in their topologically sorted order.
            let post_materialization_operations = node
                .context
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            // Check we got 4 operations both times.
            assert_eq!(pre_materialization_operations.len(), 4);
            assert_eq!(post_materialization_operations.len(), 4);
            // Check the ordering is different.
            assert_ne!(
                pre_materialization_operations,
                post_materialization_operations
            );

            // The first operation should be a CREATE.
            let create_operation = post_materialization_operations.first().unwrap();
            assert!(create_operation.is_create());

            // Reduce the operations to a document view.
            let mut document_view_fields = DocumentViewFields::new();
            for operation in post_materialization_operations {
                let fields = operation.fields().unwrap();
                for (key, value) in fields.iter() {
                    let document_view_value = DocumentViewValue::new(operation.id(), value);
                    document_view_fields.insert(key, document_view_value);
                }
            }

            // Retrieve the expected document from the store.
            let expected_document = node
                .context
                .store
                .get_document(document_id)
                .await
                .unwrap()
                .unwrap();

            // The fields should be the same.
            assert_eq!(document_view_fields, *expected_document.fields().unwrap());
        })
    }
}
