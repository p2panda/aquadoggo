// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::symlink;

use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::config::{BLOBS_DIR_NAME, BLOBS_SYMLINK_DIR_NAME};
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

    let mut is_current_view = false;
    let input_view_id = match input {
        TaskInput::SpecificView(view_id) => view_id,
        TaskInput::CurrentView(view_id) => {
            is_current_view = true;
            view_id
        }
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
            let document = context
                .store
                .get_document_by_view_id(&input_view_id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?
                .unwrap();
            Ok(vec![document])
        }

        // This task is about an updated blob piece document that may be used in one or more blob documents.
        SchemaId::BlobPiece(_) => {
            is_current_view = true;
            get_related_blobs(&input_view_id, &context).await
        }
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

    // Materialize all updated blobs to the filesystem.
    for blob_document in updated_blobs.iter() {
        // Get the raw blob data.
        let blob_data = context
            .store
            .get_blob(blob_document.view_id())
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?
            .unwrap();

        // Compose, and when needed create, the path for the blob file.
        let base_path = match &context.config.base_path {
            Some(base_path) => base_path,
            None => return Err(TaskError::Critical("No base path configured".to_string())),
        };

        let blob_dir = base_path
            .join(BLOBS_DIR_NAME)
            .join(blob_document.id().as_str());

        fs::create_dir_all(&blob_dir).map_err(|err| TaskError::Critical(err.to_string()))?;
        let blob_view_path = blob_dir.join(blob_document.view_id().to_string());

        // Write the blob to the filesystem.
        info!("Creating blob at path {blob_view_path:?}");

        let mut file = File::create(&blob_view_path).unwrap();
        file.write_all(blob_data.as_bytes()).unwrap();

        // create a symlink from `/documents/document_id -> /document_id/current_view_id`
        if is_current_view {
            info!("Creating symlink from document id to current view");

            let link_path = base_path
                .join(BLOBS_DIR_NAME)
                .join(BLOBS_SYMLINK_DIR_NAME)
                .join(blob_document.id().as_str());

            let _ = fs::remove_file(&link_path);

            symlink(blob_view_path, link_path)
                .map_err(|err| TaskError::Critical(err.to_string()))?;
        }
    }

    Ok(None)
}

/// Retrieve blobs that use the targeted blob piece as one of their fields.
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
