// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentBuilder;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A reduce task is dispatched for every entry and operation pair which arrives at a node. They
/// may also be dispatched from a dependency task when a pinned relations is present on an already
/// materialised document view.
///
/// After succesfully reducing and storing a document view an array of dependency tasks is returned.
/// If invalid inputs were passed or a fatal db error occured a critical error is returned.
pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    // Parse the task input, if they are invalid (both or neither ids provided) we critically fail
    // the task at this point. If only a document_view was passed we retrieve the document_id as
    // it is needed later.
    let document_id = match (&input.document_id, &input.document_view_id) {
        (Some(document_id), None) => Ok(document_id.to_owned()),
        (None, Some(document_view_id)) => {
            // TODO: We can skip this step if we implement https://github.com/p2panda/aquadoggo/issues/148
            let operation_id = document_view_id.clone().into_iter().next().unwrap();
            match context
                .store
                .get_document_by_operation_id(&operation_id)
                .await
                .map_err(|_| TaskError::Critical)?
            {
                Some(document_id) => Ok(document_id),
                None => return Ok(None),
            }
        }
        (_, _) => Err(TaskError::Critical),
    }?;

    // Get all operations for the requested document.
    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|_| TaskError::Critical)?;

    let document_view_id = match &input.document_view_id {
        // If this task was passed a document_view_id as input then we want to build to document only to the
        // requested view.
        Some(document_view_id) => {
            let document = match DocumentBuilder::new(operations)
                .build_to_view_id(Some(document_view_id.to_owned()))
            {
                Ok(document) => {
                    // If the document was deleted, then we return nothing.
                    if document.is_deleted() {
                        return Ok(None);
                    };
                    document
                }
                Err(_) => return Ok(None),
            };

            // Insert the document document_view into the database.
            context
                .store
                .insert_document_view(document.view().unwrap(), document.schema())
                .await
                .map_err(|_| TaskError::Critical)?;

            // Return the view id to be used in the resulting dependency task.
            document.view_id().to_owned()
        }
        // If no document_view_id was passed, this is a document_id reduce task.
        None => match DocumentBuilder::new(operations).build() {
            Ok(document) => {
                // Insert this document into storage. If it already existed, this will update it's current
                // view.
                context
                    .store
                    .insert_document(&document)
                    .await
                    .map_err(|_| TaskError::Critical)?;

                if document.is_deleted() {
                    return Ok(None);
                }
                // Return the document_view id to be used in the resulting dependency task.
                document.view_id().to_owned()
            }
            Err(_) => return Ok(None),
        },
    };

    Ok(Some(vec![Task::new(
        "dependency",
        TaskInput::new(None, Some(document_view_id)),
    )]))
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
    use p2panda_rs::operation::{AsVerifiedOperation, OperationValue};
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use rstest::rstest;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::db::stores::test_utils::{test_db, TestSqlStore};
    use crate::db::traits::DocumentStore;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;

    #[rstest]
    #[tokio::test]
    async fn reduces_documents(
        #[from(test_db)]
        #[with(2, 20, false, TEST_SCHEMA_ID.parse().unwrap(), vec![("username", OperationValue::Text("panda".into()))], vec![("username", OperationValue::Text("PANDA".into()))])]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store, Configuration::default());

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
    }

    #[rstest]
    #[tokio::test]
    async fn reduces_document_to_specific_view_id(
        #[from(test_db)]
        #[with(2, 1, false, TEST_SCHEMA_ID.parse().unwrap(), vec![("username", OperationValue::Text("panda".into()))], vec![("username", OperationValue::Text("PANDA".into()))])]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;

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
    }

    #[rstest]
    #[tokio::test]
    async fn deleted_documents_have_no_view(
        #[from(test_db)]
        #[with(3, 20, true)]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
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
    }

    #[rstest]
    #[case(test_db(3, 1, false, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("username", OperationValue::Text("panda".into()))], vec![("username", OperationValue::Text("PANDA".into()))]), true)]
    // This document is deleted, it shouldn't spawn a dependency task.
    #[case(test_db(3, 1, true, TEST_SCHEMA_ID.parse().unwrap(),
        vec![("username", OperationValue::Text("panda".into()))], vec![("username", OperationValue::Text("PANDA".into()))]), false)]
    #[tokio::test]
    async fn returns_dependency_task_inputs(
        #[case]
        #[future]
        db: TestSqlStore,
        #[case] is_next_task: bool,
    ) {
        let db = db.await;
        let context = Context::new(db.store.clone(), Configuration::default());
        let document_id = db.documents[0].clone();

        let input = TaskInput::new(Some(document_id.clone()), None);
        let next_task_inputs = reduce_task(context.clone(), input).await.unwrap();

        assert_eq!(next_task_inputs.is_some(), is_next_task);
    }

    #[rstest]
    #[should_panic(expected = "Critical")]
    #[case(None, None)]
    #[tokio::test]
    async fn fails_correctly(
        #[case] document_id: Option<DocumentId>,
        #[case] document_view_id: Option<DocumentViewId>,
        #[from(test_db)]
        #[future]
        db: TestSqlStore,
    ) {
        let db = db.await;
        let context = Context::new(db.store, Configuration::default());
        let input = TaskInput::new(document_id, document_view_id);

        reduce_task(context.clone(), input).await.unwrap();
    }
}
