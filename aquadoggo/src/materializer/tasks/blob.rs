// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::context::Context;
use crate::db::types::StorageDocument;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A blob task assembles and persists blobs to the filesystem.
///
/// Blob tasks are dispatched whenever a blob or blob piece document has all its immediate
/// dependencies available in the store.
pub async fn blob_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let input_view_id = match input {
        TaskInput::DocumentViewId(view_id) => view_id,
        _ => return Err(TaskError::Critical("Invalid task input".into())),
    };

    // Determine the schema of the updated view id.
    let schema = context
        .store
        .get_schema_by_document_view(&input_view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .unwrap();

    let updated_blobs: Vec<StorageDocument> = match schema {
        // This task is about an updated blob document so we only handle that.
        SchemaId::Blob(_) => {
            let document = context.store.get_document_by_view_id(&input_view_id).await.map_err(|err| TaskError::Critical(err.to_string()))?.unwrap();
            Ok(vec![document])
        },

        // This task is about an updated blob piece document that may be used in one or more blob documents.
        SchemaId::BlobPiece(_) => get_related_blobs(&input_view_id, &context).await,
        _ => Err(TaskError::Critical(format!(
            "Unknown system schema id: {}",
            schema
        ))),
    }?;

    // The related blobs are not known yet to this node so we mark this task failed.
    if updated_blobs.is_empty() {
        return Err(TaskError::Failure(
            "Related blob does not exist (yet)".into(),
        ));
    }

    for blob in updated_blobs.iter() {
        // @TODO: Materialize blobs to filesystem
    }

    Ok(None)
}

/// Retrieve schema definitions that use the targeted schema field definition as one of their
/// fields.
async fn get_related_blobs(
    target_blob_piece: &DocumentViewId,
    context: &Context,
) -> Result<Vec<StorageDocument>, TaskError> {
    // Retrieve all blob documents from the store
    let blobs = context
        .store
        .get_documents_by_schema(&SchemaId::Blob(1))
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))
        .unwrap();

    // Collect all blobs that use the targeted blob piece
    let mut related_blobs = vec![];
    for blob in blobs {
        // We can unwrap the value here as all documents returned from the storage method above
        // have a current view (they are not deleted).
        let fields_value = blob.get("pieces").unwrap();

        if let OperationValue::PinnedRelationList(fields) = fields_value {
            if fields
                .iter()
                .any(|field_view_id| field_view_id == target_blob_piece)
            {
                related_blobs.push(blob)
            } else {
                continue;
            }
        } else {
            // Abort if there are blobs in the store that don't match the blob schema.
            Err(TaskError::Critical(
                "Blob operation does not have a 'pieces' operation field".into(),
            ))?
        }
    }

    Ok(related_blobs)
}
