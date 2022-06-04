// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentBuilder;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    let document_id = match (&input.document_id, &input.document_view_id) {
        (Some(document_id), None) => Ok(document_id.to_owned()),
        // TODO: Alt approach: we could have a `get_operations_by_document_view_id()` in `OperationStore`, or
        // could we even do this with some fancy recursive SQL query? We might need the `previous_operations`
        // table back for that.
        (None, Some(document_view_id)) => {
            let operation_id = document_view_id.clone().into_iter().next().unwrap();
            let document_id = context
                .store
                .get_document_by_operation_id(operation_id)
                .await
                .map_err(|_| TaskError::Failure)?;
            match document_id {
                Some(id) => Ok(id),
                None => Err(TaskError::Failure),
            }
        }
        (_, _) => todo!(),
    }?;

    let operations = context
        .store
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|_| TaskError::Failure)?
        .into_iter()
        // TODO: we can avoid this conversion if we do https://github.com/p2panda/p2panda/issues/320
        .map(|op| op.into())
        .collect();

    match &input.document_view_id {
        Some(document_view_id) => {
            // TODO: If we are resolving a document_view_id, but we are missing operations from it's graph,
            // what do we want to return?
            let document = DocumentBuilder::new(operations)
                .build_to_view_id(Some(document_view_id.to_owned()))
                .map_err(|_| TaskError::Failure)?;

            // If the document is deleted, there is no view, so we don't insert it.
            if document.is_deleted() {
                return Ok(None);
            }

            context
                .store
                // Unwrap as all not deleted documents have a view.
                .insert_document_view(&document.view().unwrap(), document.schema())
                .await
                .map_err(|_| TaskError::Failure)?
        }
        None => {
            let document = DocumentBuilder::new(operations)
                .build()
                .map_err(|_| TaskError::Failure)?;

            context
                .store
                .insert_document(&document)
                .await
                .map_err(|_| TaskError::Failure)?
        }
    };

    Ok(None)
}
