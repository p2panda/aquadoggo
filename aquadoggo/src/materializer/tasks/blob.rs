// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::symlink;

use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};

use crate::config::{BLOBS_DIR_NAME, BLOBS_SYMLINK_DIR_NAME};
use crate::context::Context;
use crate::db::types::StorageDocument;
use crate::db::SqlStore;
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
            let document = context
                .store
                .get_document_by_view_id(&input_view_id)
                .await
                .map_err(|err| TaskError::Failure(err.to_string()))?
                .unwrap();
            Ok(vec![document])
        }

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

    // Materialize all updated blobs to the filesystem.
    for blob_document in updated_blobs.iter() {
        // Get the raw blob data.
        let blob_data = context
            .store
            .get_blob_by_view_id(blob_document.view_id())
            .await
            // We don't raise a critical error here, as it is possible that this method returns an
            // error.
            .map_err(|err| TaskError::Failure(err.to_string()))?
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

        // create a symlink from `../documents/<document_id>` -> `../<document_id>/<current_view_id>`
        if is_current_view(&context.store, blob_document.view_id()).await? {
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
            // It is a critical if there are blobs in the store that don't match the blob schema.
            Err(TaskError::Critical(
                "Blob operation does not have a 'pieces' operation field".into(),
            ))?
        }
    }

    Ok(related_blobs)
}

// Check if this is the current view for this blob.
async fn is_current_view(
    store: &SqlStore,
    document_view_id: &DocumentViewId,
) -> Result<bool, TaskError> {
    let blob_document_id = store
        .get_document_id_by_operation_id(document_view_id.graph_tips().first().unwrap())
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .expect("Document for blob exists");

    let current_blob_document = store
        .get_document(&blob_document_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .expect("Document for blob exists");

    Ok(current_blob_document.view_id() == document_view_id)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::config::{BLOBS_DIR_NAME, BLOBS_SYMLINK_DIR_NAME};
    use crate::materializer::tasks::blob_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{add_document, test_runner, TestNode};

    #[rstest]
    fn materializes_blob_to_filesystem(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".to_string();

            // Publish blob pieces and blob.
            let blob_piece_view_id_1 = add_document(
                &mut node,
                &SchemaId::BlobPiece(1),
                vec![("data", blob_data[..5].into())],
                &key_pair,
            )
            .await;

            let blob_piece_view_id_2 = add_document(
                &mut node,
                &SchemaId::BlobPiece(1),
                vec![("data", blob_data[5..].into())],
                &key_pair,
            )
            .await;

            // Publish blob.
            let blob_view_id = add_document(
                &mut node,
                &SchemaId::Blob(1),
                vec![
                    ("length", { blob_data.len() as i64 }.into()),
                    ("mime_type", "text/plain".into()),
                    (
                        "pieces",
                        vec![blob_piece_view_id_1, blob_piece_view_id_2].into(),
                    ),
                ],
                &key_pair,
            )
            .await;

            // Run blob task.
            let result = blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await;

            // It shouldn't fail.
            assert!(result.is_ok(), "{:#?}", result);
            // It should return no extra tasks.
            assert!(result.unwrap().is_none());

            // Convert blob view id to document id.
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Construct the expected path to the blob view file.
            let base_path = node.context.config.base_path.as_ref().unwrap();
            let blob_path = base_path
                .join(BLOBS_DIR_NAME)
                .join(document_id.as_str())
                .join(blob_view_id.to_string());

            // Read from this file
            let retrieved_blob_data = fs::read_to_string(blob_path);

            // It should match the complete published blob data.
            assert!(retrieved_blob_data.is_ok(), "{:?}", retrieved_blob_data);
            assert_eq!(blob_data, retrieved_blob_data.unwrap());

            // Construct the expected path to the blob symlink file location.
            let blob_path = base_path
                .join(BLOBS_DIR_NAME)
                .join(BLOBS_SYMLINK_DIR_NAME)
                .join(document_id.as_str());

            // Read from this file
            let retrieved_blob_data = fs::read_to_string(blob_path);

            // It should match the complete published blob data.
            assert!(retrieved_blob_data.is_ok(), "{:?}", retrieved_blob_data);
            assert_eq!(blob_data, retrieved_blob_data.unwrap())
        })
    }
}
