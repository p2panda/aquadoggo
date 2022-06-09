// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentBuilder;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{Task, TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    let document_id = match (&input.document_id, &input.document_view_id) {
        (Some(document_id), None) => Ok(document_id.to_owned()),
        // TODO: Alt approach: we could have a `get_operations_by_document_view_id()` in `OperationStore`, or
        // could we even do this with some fancy recursive SQL query? We might need the `previous_operations`
        // table back for that.
        (None, Some(document_view_id)) => {
            let operation_id = document_view_id.clone().into_iter().next().unwrap();
            match context
                .store
                .get_document_by_operation_id(operation_id)
                .await
                .map_err(|_| TaskError::Critical)? // We only get an error here on a critical database error.
            {
                Some(document_id) => Ok(document_id),
                None => Err(TaskError::Failure),
            }
        }
        (_, _) => Err(TaskError::Failure),
    }?;

    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|_| TaskError::Critical)?
        .into_iter()
        // TODO: we can avoid this conversion if we do https://github.com/p2panda/p2panda/issues/320
        .map(|op| op.into())
        .collect();

    let document_view_id = match &input.document_view_id {
        Some(document_view_id) => {
            // TODO: If we are resolving a document_view_id, but we are missing operations from it's graph,
            // what do we want to return?
            let document = DocumentBuilder::new(operations)
                .build_to_view_id(Some(document_view_id.to_owned()))
                .map_err(|_| TaskError::Critical)?;

            if document.is_deleted() {
                return Ok(None);
            } else {
                context
                    .store
                    // Unwrap as all not deleted documents have a view.
                    .insert_document_view(document.view().unwrap(), document.schema())
                    .await
                    .map_err(|_| TaskError::Critical)?;

                document.view_id().to_owned()
            }
        }
        None => {
            let document = DocumentBuilder::new(operations)
                .build()
                .map_err(|_| TaskError::Failure)?;

            context
                .store
                .insert_document(&document)
                .await
                .map_err(|_| TaskError::Failure)?;

            if document.is_deleted() {
                return Ok(None);
            } else {
                document.view_id().to_owned()
            }
        }
    };

    Ok(Some(vec![Task::new(
        "dependency",
        TaskInput::new(None, Some(document_view_id)),
    )]))
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::operation::OperationValue;
    use p2panda_rs::storage_provider::traits::{AsStorageOperation, OperationStore};
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::{random_document_id, random_document_view_id};
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

        let mut document_operations = db
            .store
            .get_operations_by_document_id(&db.documents[0])
            .await
            .unwrap();

        let document_view_id: DocumentViewId = document_operations.pop().unwrap().id().into();

        let context = Context::new(db.store, Configuration::default());
        let input = TaskInput::new(None, Some(document_view_id.clone()));

        assert!(reduce_task(context.clone(), input).await.is_ok());

        let document_view = context
            .store
            .get_document_view_by_id(&document_view_id)
            .await
            .unwrap();

        assert_eq!(
            document_view.unwrap().get("username").unwrap().value(),
            &OperationValue::Text("PANDA".to_string())
        );

        // We didn't reduce this document_view_id so it shouldn't exist in the db.
        let document_view_id: DocumentViewId = document_operations.pop().unwrap().id().into();
        let document_view = context
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
        let context = Context::new(db.store, Configuration::default());

        for document_id in &db.documents {
            let input = TaskInput::new(Some(document_id.clone()), None);
            assert!(reduce_task(context.clone(), input).await.is_ok());
        }

        for document_id in &db.documents {
            let document_view = context.store.get_document_by_id(document_id).await.unwrap();

            assert!(document_view.is_none())
        }
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
    #[should_panic(expected = "Failure")]
    #[case(None, None)]
    #[should_panic(expected = "Failure")]
    #[case(None, Some(random_document_view_id()))]
    #[should_panic(expected = "Failure")]
    #[case(Some(random_document_id()), None)]
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
