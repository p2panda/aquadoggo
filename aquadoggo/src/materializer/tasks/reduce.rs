// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::operation::VerifiedOperation;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A reduce task is dispatched for every entry and operation pair which arrives at a node.
///
/// They may also be dispatched from a dependency task when a pinned relations is present on an
/// already materialised document view.
///
/// After succesfully reducing and storing a document view an array of dependency tasks is returned.
/// If invalid inputs were passed or a fatal db error occured a critical error is returned.
pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    // Find out which document we are handling
    let document_id = resolve_document_id(&context, &input).await?;

    // Get all operations for the requested document
    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|_| TaskError::Critical)?;

    let document_view_id = match &input.document_view_id {
        // If this task was passed a document_view_id as input then we want to build to document
        // only to the requested view
        Some(view_id) => reduce_document_view(&context, view_id, operations).await?,
        // If no document_view_id was passed, this is a document_id reduce task.
        None => reduce_document(&context, operations).await?,
    };

    // Dispatch a "dependency" task if we created a new document view
    match document_view_id {
        Some(view_id) => Ok(Some(vec![Task::new(
            "dependency",
            TaskInput::new(None, Some(view_id)),
        )])),
        None => Ok(None),
    }
}

/// Helper method to resolve a `DocumentId` from task input.
///
/// If the task input is invalid (both document_id and document_view_id missing or given) we
/// critically fail the task at this point. If only a document_view_id was passed we retrieve the
/// document_id as it is needed later.
async fn resolve_document_id(
    context: &Context,
    input: &TaskInput,
) -> Result<DocumentId, TaskError> {
    match (&input.document_id, &input.document_view_id) {
        // The `DocumentId` is already given, we don't have to do anything
        (Some(document_id), None) => Ok(document_id.to_owned()),

        // A `DocumentViewId` is given, let's find out its document id
        (None, Some(document_view_id)) => {
            // @TODO: We can skip this step if we implement:
            // https://github.com/p2panda/aquadoggo/issues/148
            let operation_id = document_view_id.clone().into_iter().next().unwrap();
            match context
                .store
                .get_document_by_operation_id(&operation_id)
                .await
                .map_err(|_| TaskError::Critical)?
            {
                Some(document_id) => Ok(document_id),
                None => Err(TaskError::Critical),
            }
        }

        // None or both have been provided which smells like a bug
        (_, _) => Err(TaskError::Critical),
    }
}

/// Helper method to reduce an operation graph to a specific document view, returning the
/// `DocumentViewId` of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document_view(
    context: &Context,
    document_view_id: &DocumentViewId,
    operations: Vec<VerifiedOperation>,
) -> Result<Option<DocumentViewId>, TaskError> {
    let document = match DocumentBuilder::new(operations)
        .build_to_view_id(Some(document_view_id.to_owned()))
    {
        Ok(document) => {
            // If the document was deleted, then we return nothing
            if document.is_deleted() {
                return Ok(None);
            };

            document
        }
        Err(_) => {
            // There is not enough operations yet to materialise this view. Maybe next time!
            return Ok(None);
        }
    };

    // Insert the new document view into the database
    context
        .store
        .insert_document_view(document.view().unwrap(), document.schema())
        .await
        .map_err(|_| TaskError::Critical)?;

    // Return the new view id to be used in the resulting dependency task
    Ok(Some(document.view_id().to_owned()))
}

/// Helper method to reduce an operation graph to the latest document view, returning the
/// `DocumentViewId` of the just created new document view.
///
/// It returns `None` if either that document view reached "deleted" status or we don't have enough
/// operations to materialise.
async fn reduce_document(
    context: &Context,
    operations: Vec<VerifiedOperation>,
) -> Result<Option<DocumentViewId>, TaskError> {
    match DocumentBuilder::new(operations).build() {
        Ok(document) => {
            // Insert this document into storage. If it already existed, this will update it's
            // current view
            context
                .store
                .insert_document(&document)
                .await
                .map_err(|_| TaskError::Critical)?;

            // If the document was deleted, then we return nothing
            if document.is_deleted() {
                return Ok(None);
            }

            // Return the new document_view id to be used in the resulting dependency task
            Ok(Some(document.view_id().to_owned()))
        }
        Err(_) => {
            // There is not enough operations yet to materialise this view. Maybe next time!
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
    use p2panda_rs::entry::{sign_and_encode, Entry};
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{
        AsVerifiedOperation, OperationEncoded, OperationValue, VerifiedOperation,
    };
    use p2panda_rs::storage_provider::traits::{
        AsStorageEntry, EntryStore, OperationStore, StorageProvider,
    };
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{operation, operation_fields};
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::db::stores::StorageEntry;
    use crate::db::traits::DocumentStore;
    use crate::graphql::client::EntryArgsRequest;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;

    #[rstest]
    fn reduces_documents(
        #[from(test_db)]
        #[with(
            2,
            20,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![("username", OperationValue::Text("panda".into()))],
            vec![("username", OperationValue::Text("PANDA".into()))]
        )]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(db.store.clone(), Configuration::default());

            for document_id in &db.documents {
                let input = TaskInput::new(Some(document_id.clone()), None);
                assert!(reduce_task(context.clone(), input).await.is_ok());
            }

            for document_id in &db.documents {
                let document_view = context.store.get_document_by_id(document_id).await.unwrap();

                assert_eq!(
                    document_view.unwrap().get("username").unwrap().value(),
                    &OperationValue::Text("PANDA".to_string())
                )
            }
        });
    }

    #[rstest]
    fn updates_a_document(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_id = db.documents.first().unwrap();
            let key_pair = db.key_pairs.first().unwrap();
            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            let context = Context::new(db.store.clone(), Configuration::default());
            let input = TaskInput::new(Some(document_id.clone()), None);

            // There is one CREATE operation for this document in the db, it should create a document
            // in the documents table.
            assert!(reduce_task(context.clone(), input.clone()).await.is_ok());

            // Now we create and insert an UPDATE operation for this document.
            let entry_args = db
                .store
                .get_entry_args(&EntryArgsRequest {
                    author,
                    document: Some(document_id.clone()),
                })
                .await
                .unwrap();

            let operation = operation(
                Some(operation_fields(vec![(
                    "username",
                    OperationValue::Text("meeeeeee".to_string()),
                )])),
                Some(document_id.as_str().parse().unwrap()),
                None,
            );

            let entry = Entry::new(
                &entry_args.log_id,
                Some(&operation),
                entry_args.skiplink.as_ref(),
                entry_args.backlink.as_ref(),
                &entry_args.seq_num,
            )
            .unwrap();

            let entry_signed = sign_and_encode(&entry, key_pair).unwrap();
            let operation_encoded = OperationEncoded::try_from(&operation).unwrap();

            db.store
                .insert_entry(StorageEntry::new(&entry_signed, &operation_encoded).unwrap())
                .await
                .unwrap();

            let verified_operation =
                VerifiedOperation::new_from_entry(&entry_signed, &operation_encoded).unwrap();

            db.store
                .insert_operation(&verified_operation, document_id)
                .await
                .unwrap();

            // This should now find the new UPDATE operation and perform an update on the document
            // in the documents table.
            assert!(reduce_task(context.clone(), input).await.is_ok());

            // The new view should exist and the document should refer to it.
            let document_view = context.store.get_document_by_id(document_id).await.unwrap();
            assert_eq!(
                document_view.unwrap().get("username").unwrap().value(),
                &OperationValue::Text("meeeeeee".to_string())
            )
        })
    }

    #[rstest]
    fn reduces_document_to_specific_view_id(
        #[from(test_db)]
        #[with(
            2,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![("username", OperationValue::Text("panda".into()))],
            vec![("username", OperationValue::Text("PANDA".into()))]
        )]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_operations = db
                .store
                .get_operations_by_document_id(&db.documents[0])
                .await
                .unwrap();

            let document = DocumentBuilder::new(document_operations).build().unwrap();
            let mut sorted_document_operations = document.operations().clone();

            let document_view_id: DocumentViewId = sorted_document_operations
                .pop()
                .unwrap()
                .operation_id()
                .clone()
                .into();

            let context = Context::new(db.store.clone(), Configuration::default());
            let input = TaskInput::new(None, Some(document_view_id.clone()));

            assert!(reduce_task(context.clone(), input).await.is_ok());

            let document_view = db
                .store
                .get_document_view_by_id(&document_view_id)
                .await
                .unwrap();

            assert_eq!(
                document_view.unwrap().get("username").unwrap().value(),
                &OperationValue::Text("PANDA".to_string())
            );

            // We didn't reduce this document_view_id so it shouldn't exist in the db.
            let document_view_id: DocumentViewId = sorted_document_operations
                .pop()
                .unwrap()
                .operation_id()
                .clone()
                .into();

            let document_view = db
                .store
                .get_document_view_by_id(&document_view_id)
                .await
                .unwrap();

            assert!(document_view.is_none());
        });
    }

    #[rstest]
    fn deleted_documents_have_no_view(
        #[from(test_db)]
        #[with(3, 20, true)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(db.store.clone(), Configuration::default());

            for document_id in &db.documents {
                let input = TaskInput::new(Some(document_id.clone()), None);
                let tasks = reduce_task(context.clone(), input).await.unwrap();
                assert!(tasks.is_none());
            }

            for document_id in &db.documents {
                let document_view = context.store.get_document_by_id(document_id).await.unwrap();
                assert!(document_view.is_none())
            }

            let document_operations = context
                .store
                .get_operations_by_document_id(&db.documents[0])
                .await
                .unwrap();

            let document = DocumentBuilder::new(document_operations).build().unwrap();

            let input = TaskInput::new(None, Some(document.view_id().clone()));
            let tasks = reduce_task(context.clone(), input).await.unwrap();

            assert!(tasks.is_none());
        });
    }

    #[rstest]
    #[case(
        test_db(
            3,
            1,
            false,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![("username", OperationValue::Text("panda".into()))],
            vec![("username", OperationValue::Text("PANDA".into()))]
        ),
        true
    )]
    // This document is deleted, it shouldn't spawn a dependency task.
    #[case(
        test_db(
            3,
            1,
            true,
            TEST_SCHEMA_ID.parse().unwrap(),
            vec![("username", OperationValue::Text("panda".into()))],
            vec![("username", OperationValue::Text("PANDA".into()))]
        ),
        false
    )]
    fn returns_dependency_task_inputs(
        #[case] runner: TestDatabaseRunner,
        #[case] is_next_task: bool,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let context = Context::new(db.store.clone(), Configuration::default());
            let document_id = db.documents[0].clone();

            let input = TaskInput::new(Some(document_id.clone()), None);
            let next_task_inputs = reduce_task(context.clone(), input).await.unwrap();

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
            let context = Context::new(db.store.clone(), Configuration::default());
            let input = TaskInput::new(document_id, document_view_id);

            assert!(reduce_task(context.clone(), input).await.is_err());
        });
    }
}
