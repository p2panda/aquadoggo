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

    let document_view_id = match &input.document_view_id {
        // If this task was passed a document_view_id as input then we want to build to document
        // only to the requested view
        Some(view_id) => reduce_document_view(&context, view_id, &operations).await?,
        // If no document_view_id was passed, this is a document_id reduce task.
        None => reduce_document(&context, &operations).await?,
    };

    // Dispatch a "dependency" task if we created a new document view
    match document_view_id {
        Some(view_id) => {
            debug!("Dispatch dependency task for view with id: {}", view_id);

            Ok(Some(vec![Task::new(
                "dependency",
                TaskInput::new(None, Some(view_id)),
            )]))
        }
        None => {
            debug!("No dependency tasks to dispatch");
            Ok(None)
        }
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

            let operation_id = document_view_id.iter().next().unwrap();

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
    document_view_id: &DocumentViewId,
    operations: &Vec<O>,
) -> Result<Option<DocumentViewId>, TaskError> {
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

    // Return the new view id to be used in the resulting dependency task
    Ok(Some(document.view_id().to_owned()))
}

/// Helper method to reduce an operation graph to the latest document view, returning the
/// `DocumentViewId` of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document<O: AsOperation + WithId<OperationId> + WithPublicKey>(
    context: &Context,
    operations: &Vec<O>,
) -> Result<Option<DocumentViewId>, TaskError> {
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
            }

            // Return the new document_view id to be used in the resulting dependency task
            Ok(Some(document.view_id().to_owned()))
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
    use p2panda_rs::test_utils::memory_store::helpers::send_to_store;
    use rstest::rstest;

    use crate::test_utils::{
        doggo_fields, doggo_schema, test_db, TestDatabase, TestDatabaseRunner,
    };
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;

    #[rstest]
    fn reduces_documents(
        #[from(test_db)]
        #[with(
            2,
            1,
            20,
            false,
            doggo_schema(),
            doggo_fields(),
            vec![("username", OperationValue::String("PANDA".into()))]
        )]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            for document_id in &db.test_data.documents {
                let input = TaskInput::new(Some(document_id.clone()), None);
                assert!(reduce_task(db.context.clone(), input).await.is_ok());
            }

            for document_id in &db.test_data.documents {
                let document = db.store.get_document(document_id).await.unwrap();

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
        #[from(test_db)]
        #[with(
            1,
            1,
            1,
            false,
            constants::schema(),
            constants::test_fields(),
            constants::test_fields()
        )]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_id = db.test_data.documents.first().unwrap();
            let key_pair = db.test_data.key_pairs.first().unwrap();

            let input = TaskInput::new(Some(document_id.clone()), None);

            // There is one CREATE operation for this document in the db, it should create a document
            // in the documents table.
            assert!(reduce_task(db.context.clone(), input.clone()).await.is_ok());

            // Now we create and insert an UPDATE operation for this document.
            let (_, _) = send_to_store(
                &db.store,
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
            assert!(reduce_task(db.context.clone(), input).await.is_ok());

            // The new view should exist and the document should refer to it.
            let document = db.store.get_document(document_id).await.unwrap();
            assert_eq!(
                document.unwrap().get("username").unwrap(),
                &OperationValue::String("meeeeeee".to_string())
            )
        })
    }

    #[rstest]
    fn reduces_document_to_specific_view_id(
        #[from(test_db)]
        #[with( 2, 1, 1, false, doggo_schema(), doggo_fields(), vec![("username", OperationValue::String("PANDA".into()))])]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // The document id for a document who's operations are in the database but it hasn't been
            // materialised yet.
            let document_id = &db.test_data.documents[0];

            // Get the operations.
            let document_operations = db
                .store
                .get_operations_by_document_id(document_id)
                .await
                .unwrap();

            // Sort the operations into their ready for reducing order.
            let document_builder = DocumentBuilder::from(&document_operations);
            let sorted_document_operations = build_graph(&document_builder.operations())
                .unwrap()
                .sort()
                .unwrap()
                .sorted();

            // Reduce document to it's current view and insert into database.
            let input = TaskInput::new(Some(document_id.clone()), None);
            assert!(reduce_task(db.context.clone(), input).await.is_ok());

            // We should be able to query this specific view now and receive the expected state.
            let document_view_id: DocumentViewId =
                sorted_document_operations.get(1).unwrap().clone().0.into();
            let document = db
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

            let document = db
                .store
                .get_document_by_view_id(&document_view_id)
                .await
                .unwrap();

            assert!(document.is_none());

            // But now if we do request an earlier view is materialised for this document...
            let input = TaskInput::new(None, Some(document_view_id.clone()));
            assert!(reduce_task(db.context.clone(), input).await.is_ok());

            // Then we should now be able to query it and revieve the expected value.
            let document = db
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
        #[from(test_db)]
        #[with(3, 1, 2, true)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            for document_id in &db.test_data.documents {
                let input = TaskInput::new(Some(document_id.clone()), None);
                let tasks = reduce_task(db.context.clone(), input).await.unwrap();
                assert!(tasks.is_none());
            }

            for document_id in &db.test_data.documents {
                let document = db.store.get_document(document_id).await.unwrap();
                assert!(document.is_none())
            }

            let document_operations = db
                .store
                .get_operations_by_document_id(&db.test_data.documents[0])
                .await
                .unwrap();

            let document = Document::try_from(&document_operations).unwrap();

            let input = TaskInput::new(None, Some(document.view_id().clone()));
            let tasks = reduce_task(db.context.clone(), input).await.unwrap();

            assert!(tasks.is_none());
        });
    }

    #[rstest]
    #[case(
        test_db(3, 1, 1, false, doggo_schema(), doggo_fields(), doggo_fields()),
        true
    )]
    // This document is deleted, it shouldn't spawn a dependency task.
    #[case(
        test_db(3, 1, 1, true, doggo_schema(), doggo_fields(), doggo_fields()),
        false
    )]
    fn returns_dependency_task_inputs(
        #[case] runner: TestDatabaseRunner,
        #[case] is_next_task: bool,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let document_id = db.test_data.documents[0].clone();

            let input = TaskInput::new(Some(document_id.clone()), None);
            let next_task_inputs = reduce_task(db.context.clone(), input).await.unwrap();

            assert_eq!(next_task_inputs.is_some(), is_next_task);
        });
    }

    #[rstest]
    #[case(None, None)]
    fn fails_correctly(
        #[case] document_id: Option<DocumentId>,
        #[case] document_view_id: Option<DocumentViewId>,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let input = TaskInput::new(document_id, document_view_id);

            assert!(reduce_task(db.context.clone(), input).await.is_err());
        });
    }

    #[rstest]
    fn does_not_error_when_document_missing(
        #[from(test_db)]
        #[with(0, 0)]
        runner: TestDatabaseRunner,
        #[from(random_document_id)] document_id: DocumentId,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
    ) {
        // Prepare empty database.
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Dispatch a reduce task for a document which doesn't exist by it's document id.
            let input = TaskInput::new(Some(document_id), None);
            assert!(reduce_task(db.context.clone(), input).await.is_ok());

            // Dispatch a reduce task for a document which doesn't exist by it's document view id.
            let input = TaskInput::new(None, Some(document_view_id));
            assert!(reduce_task(db.context.clone(), input).await.is_ok());
        });
    }
}
