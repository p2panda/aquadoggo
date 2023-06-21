// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{Document, DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::operation::traits::{AsOperation, WithPublicKey};
use p2panda_rs::operation::OperationId;
use p2panda_rs::storage_provider::traits::{DocumentStore, EntryStore, LogStore, OperationStore};
use p2panda_rs::{Human, WithId};

use crate::context::Context;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// Build a materialised view for a document by reducing the document's operation graph and storing to disk.
///
/// ## Task input
///
/// If the task input contains a document view id, only this view is stored and any document id
/// also existing on the task input is ignored.
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
///
/// After succesfully reducing and storing a document view an array of dependency tasks is returned.
/// If invalid inputs were passed or a fatal db error occured a critical error is returned.
pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    // If this task is concerned with a document view then we can first check if it has actually
    // already been materialized. If so, we exit this task immediately and return no new tasks.
    if let Some(document_view_id) = &input.document_view_id {
        let document_view_exists = context
            .store
            .get_document_by_view_id(document_view_id)
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?
            .is_some();

        if document_view_exists {
            return Ok(None);
        }
    }

    // Find out which document we are handling
    let document_id = match resolve_document_id(&context, &input).await? {
        Some(document_id) => Ok(document_id),
        None => {
            debug!("No document found for this view, exit without dispatching any other tasks");
            return Ok(None);
        }
    }?;

    // Get all operations for the requested document
    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    match &input.document_view_id {
        // If this task was passed a document_view_id as input then we want to build to document
        // only to the requested view
        Some(view_id) => reduce_document_view(&context, &document_id, view_id, &operations).await,
        // If no document_view_id was passed, this is a document_id reduce task.
        None => reduce_document(&context, &operations).await,
    }
}

/// Helper method to resolve a `DocumentId` from task input.
///
/// If the task input is invalid (both document_id and document_view_id missing or given) we
/// critically fail the task at this point. If only a document_view_id was passed we retrieve the
/// document_id as it is needed later.
async fn resolve_document_id<S: EntryStore + OperationStore + LogStore + DocumentStore>(
    context: &Context<S>,
    input: &TaskInput,
) -> Result<Option<DocumentId>, TaskError> {
    match (&input.document_id, &input.document_view_id) {
        // The `DocumentId` is already given, we don't have to do anything
        (Some(document_id), None) => Ok(Some(document_id.to_owned())),

        // A `DocumentViewId` is given, let's find out its document id
        (None, Some(document_view_id)) => {
            // @TODO: We can skip this step if we implement:
            // https://github.com/p2panda/aquadoggo/issues/148
            debug!("Find document for view with id: {}", document_view_id);

            // Pick one operation from the view, this is all we need to determine the document id.
            let operation_id = document_view_id.iter().next().unwrap();

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
        // None or both have been provided which smells like a bug
        (_, _) => Err(TaskError::Critical("Invalid task input".into())),
    }
}

/// Helper method to reduce an operation graph to a specific document view, returning the
/// `DocumentViewId` of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document_view<O: AsOperation + WithId<OperationId> + WithPublicKey>(
    context: &Context,
    document_id: &DocumentId,
    document_view_id: &DocumentViewId,
    operations: &Vec<O>,
) -> Result<Option<Vec<Task<TaskInput>>>, TaskError> {
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

    // Attempt to retrieve the document this view is part of in order to determine if it has
    // been once already materialized yet.
    let existing_document = context
        .store
        .get_document(document_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    // If it wasn't found then we shouldn't reduce this view yet (as the document it's part of
    // should be reduced first). In this case we assume the task for reducing the document is
    // already in the queue and so we simply re-issue this reduce task.
    if existing_document.is_none() {
        debug!(
            "Document {} for view not materialized yet, reissuing current reduce task for {}",
            document_id.display(),
            document_view_id.display()
        );
        return Ok(Some(vec![Task::new(
            "reduce",
            TaskInput::new(None, Some(document_view_id.to_owned())),
        )]));
    };

    // Insert the new document view into the database
    context
        .store
        .insert_document_view(
            &document.view().unwrap(),
            document.id(),
            document.schema_id(),
        )
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    info!("Stored {} document view {}", document, document.view_id());

    debug!(
        "Dispatch dependency task for view with id: {}",
        document.view_id()
    );
    Ok(Some(vec![Task::new(
        "dependency",
        TaskInput::new(None, Some(document.view_id().to_owned())),
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
            // Insert this document into storage. If it already existed, this will update it's
            // current view
            context
                .store
                .insert_document(&document)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // If the document was deleted, then we return nothing
            if document.is_deleted() {
                info!(
                    "Deleted {} final view {}",
                    document.display(),
                    document.view_id().display()
                );
                return Ok(None);
            }

            if document.is_edited() {
                info!(
                    "Updated {} latest view {}",
                    document.display(),
                    document.view_id().display()
                );
            } else {
                info!("Created {}", document.display(),);
            };

            debug!(
                "Dispatch dependency task for view with id: {}",
                document.view_id()
            );
            Ok(Some(vec![Task::new(
                "dependency",
                TaskInput::new(None, Some(document.view_id().to_owned())),
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
    use p2panda_rs::document::{Document, DocumentBuilder, DocumentId, DocumentViewId};
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
    use rstest::rstest;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::{Task, TaskInput};
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
                let input = TaskInput::new(Some(document_id.clone()), None);
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

            let input = TaskInput::new(Some(document_id.clone()), None);

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
        #[with( 2, 1, 1, false, doggo_schema(), doggo_fields(), vec![("username", OperationValue::String("PANDA".into()))])]
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
            let input = TaskInput::new(Some(document_id.clone()), None);
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
            let input = TaskInput::new(None, Some(document_view_id.clone()));
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
    fn reissue_reduce_when_document_does_not_exist(
        #[from(populate_store_config)]
        #[with( 2, 1, 1, false, doggo_schema(), doggo_fields(), vec![("username", OperationValue::String("PANDA".into()))])]
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

            // Build the document.
            let document = DocumentBuilder::from(&document_operations).build().unwrap();

            // Run a reduce task for this documents current view. The operations are already in
            // the store, but the document is not materialized yet. This means we shouldn't insert
            // any views for it yet. In that case, we expect this task to issue another reduce
            // task with the same input.
            let input = TaskInput::new(None, Some(document.view_id().clone()));
            let next_tasks = reduce_task(node.context.clone(), input.clone())
                .await
                .expect("Task should succeed")
                .expect("Task to be returned");

            assert_eq!(next_tasks, vec![Task::new("reduce", input)]);
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
                let input = TaskInput::new(Some(document_id.clone()), None);
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

            let input = TaskInput::new(None, Some(document.view_id().clone()));
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
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids
                .get(0)
                .expect("There should be at least one document id");

            let input = TaskInput::new(Some(document_id.clone()), None);
            let next_task_inputs = reduce_task(node.context.clone(), input).await.unwrap();

            assert_eq!(next_task_inputs.is_some(), is_next_task);
        });
    }

    #[rstest]
    #[case(None, None)]
    fn fails_correctly(
        #[case] document_id: Option<DocumentId>,
        #[case] document_view_id: Option<DocumentViewId>,
    ) {
        test_runner(move |node: TestNode| async move {
            let input = TaskInput::new(document_id, document_view_id);

            assert!(reduce_task(node.context.clone(), input).await.is_err());
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
            let input = TaskInput::new(Some(document_id), None);
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // Dispatch a reduce task for a document which doesn't exist by it's document view id.
            let input = TaskInput::new(None, Some(document_view_id));
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
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations and build the document.
            let operations = node
                .context
                .store
                .get_operations_by_document_id(document_id)
                .await
                .unwrap();

            // Build the document from the operations.
            let document_builder = DocumentBuilder::from(&operations);
            let document = document_builder.build().unwrap();

            // Issue a reduce task for the document, which also inserts the current view.
            let input = TaskInput::new(Some(document_id.to_owned()), None);
            assert!(reduce_task(node.context.clone(), input).await.is_ok());

            // Issue a reduce task for the document view, which should succeed although no new
            // view is inserted.
            let input = TaskInput::new(None, Some(document.view_id().to_owned()));
            assert!(reduce_task(node.context.clone(), input).await.is_ok());
        })
    }
}
