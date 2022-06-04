// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentBuilder;
use p2panda_rs::storage_provider;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::DocumentStore;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn reduce_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    let document_id = match (input.document_id, input.document_view_id) {
        (Some(document_id), None) => document_id,
        // TODO: find document_id from document_view_id then get operations.
        //
        // We could have a `get_operations_by_document_view_id()` in `OperationStore`, or
        // could we even do this with some fancy recursive SQL query? We might need the `previous_operations`
        // table back for that.
        (None, Some(_)) => todo!(),
        (_, _) => todo!(),
    };

    let operations = context
        .provider
        .get_operations_by_document_id(&document_id)
        .await
        .map_err(|_| TaskError::Failure)?
        .into_iter()
        // TODO: we can avoid this conversion if we do https://github.com/p2panda/p2panda/issues/320
        .map(|op| op.into())
        .collect();

    // TODO: If we are resolving a document_view_id, but we are missing operations from it's graph,
    // what do we want to return?

    let document = DocumentBuilder::new(operations)
        .build()
        .map_err(|_| TaskError::Failure)?;

    // TODO: If this was a document_view reduction, we want to call `insert_document_view()` instead.

    context
        .provider
        .insert_document(&document)
        .await
        .map_err(|_| TaskError::Failure)?;

    Ok(None)
}
