// SPDX-License-Identifier: AGPL-3.0-or-later

use futures::{pin_mut, StreamExt};
use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

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
            let document = context
                .store
                .get_document_by_view_id(&input_view_id)
                .await
                .map_err(|err| TaskError::Failure(err.to_string()))?
                .unwrap();
            Ok(vec![document])
        }

        // This task is about an updated blob piece document that may be used in one or more blob
        // documents.
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
        // Get a stream of raw blob data
        let mut blob_stream = context
            .store
            .get_blob_by_view_id(blob_document.view_id())
            .await
            // We don't raise a critical error here, as it is possible that this method returns an
            // error, for example when not all blob pieces are available yet for materialisation
            .map_err(|err| TaskError::Failure(err.to_string()))?
            .expect("Blob data exists at this point");

        // Determine a path for this blob file on the file system
        let blob_view_path = context
            .config
            .blobs_base_path
            .join(blob_document.view_id().to_string());

        // Write the blob to the filesystem
        info!("Creating blob at path {}", blob_view_path.display());

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&blob_view_path)
            .await
            .map_err(|err| {
                TaskError::Critical(format!(
                    "Could not create blob file @ {}: {}",
                    blob_view_path.display(),
                    err
                ))
            })?;

        // Read from the stream, chunk by chunk, and write every part to the file. This should put
        // less pressure on our systems memory and allow writing large blob files
        let stream = blob_stream.read_all();
        pin_mut!(stream);

        while let Some(value) = stream.next().await {
            match value {
                Ok(buf) => file.write(&buf).await.map_err(|err| {
                    TaskError::Critical(format!(
                        "Error occurred when writing to blob file @ {}: {}",
                        blob_view_path.display(),
                        err
                    ))
                }),
                Err(err) => Err(TaskError::Failure(format!(
                    "Blob data is invalid and can not be materialised: {}",
                    err
                ))),
            }?;
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

#[cfg(test)]
mod tests {
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;
    use tokio::fs;

    use crate::materializer::tasks::blob_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{add_blob, test_runner, TestNode};

    #[rstest]
    fn materializes_blob_to_filesystem(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish blob
            let blob_data = "Hello, World!";
            let blob_view_id =
                add_blob(&mut node, blob_data.as_bytes(), 5, "plain/text", &key_pair).await;

            // Run blob task
            let result = blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await;

            // It shouldn't fail
            assert!(result.is_ok(), "{:#?}", result);
            // It should return no extra tasks
            assert!(result.unwrap().is_none());

            // Construct the expected path to the blob view file
            let base_path = &node.context.config.blobs_base_path;
            let blob_path = base_path.join(blob_view_id.to_string());

            // Read from this file
            let retrieved_blob_data = fs::read_to_string(blob_path).await;

            // It should match the complete published blob data
            assert!(retrieved_blob_data.is_ok(), "{:?}", retrieved_blob_data);
            assert_eq!(blob_data, retrieved_blob_data.unwrap());
        })
    }
}
