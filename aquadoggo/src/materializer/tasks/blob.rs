// SPDX-License-Identifier: AGPL-3.0-or-later

use futures::{pin_mut, StreamExt};
use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A blob task assembles and persists blobs to the filesystem.
///
/// Blob tasks are dispatched whenever a blob document has its dependencies (pieces) available in
/// the store.
pub async fn blob_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let input_view_id = match input {
        TaskInput::DocumentViewId(view_id) => view_id,
        _ => return Err(TaskError::Critical("Invalid task input".into())),
    };

    let blob_document = context
        .store
        .get_document_by_view_id(&input_view_id)
        .await
        .map_err(|err| TaskError::Failure(err.to_string()))?;

    match blob_document {
        Some(blob_document) => {
            // This document should be a blob document. If it isn't, critically fail the task now
            if !matches!(blob_document.schema_id(), SchemaId::Blob(_)) {
                return Err(TaskError::Critical(format!(
                    "Unexpected system schema id: {}",
                    blob_document.schema_id()
                )));
            }

            // Determine a path for this blob file on the file system
            let blob_view_path = context
                .config
                .blobs_base_path
                .join(blob_document.view_id().to_string());

            // Check if the blob has already been materialized and return early from this task
            // with an error if it has.
            let is_blob_materialized = OpenOptions::new()
                .read(true)
                .open(&blob_view_path)
                .await
                .is_ok();
            if is_blob_materialized {
                return Err(TaskError::Failure(format!(
                    "Blob file already exists at {}",
                    blob_view_path.display()
                )));
            }

            // Get a stream of raw blob data
            let mut blob_stream = context
                .store
                .get_blob_by_view_id(blob_document.view_id())
                .await
                // We don't raise a critical error here, as it is possible that this method returns an
                // error, for example when not all blob pieces are available yet for materialisation
                .map_err(|err| TaskError::Failure(err.to_string()))?
                .expect("Blob data exists at this point");

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
                    Ok(buf) => file.write_all(&buf).await.map_err(|err| {
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
        // If the blob document did not exist yet in the store we fail this task.
        None => {
            return Err(TaskError::Failure("Blob does not exist (yet)".into()));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::storage_provider::traits::DocumentStore;
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

            // Run the blob task again, it should return an error as the blob was already
            // materialized once.
            let result = blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await;

            assert!(result.is_err(), "{:?}", result,);
        })
    }

    #[rstest]
    fn materializes_larger_blob_to_filesystem(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish blob
            let length = 10e6 as u32; // 5MB
            let blob_data: Vec<u8> = (0..length).map(|_| rand::random::<u8>()).collect();
            let blob_view_id = add_blob(
                &mut node,
                &blob_data,
                256 * 1000,
                "application/octet-stream",
                &key_pair,
            )
            .await;

            // Run blob task
            let result = blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await;

            // It shouldn't fail
            assert!(result.is_ok());
            // It should return no extra tasks
            assert!(result.unwrap().is_none());

            // Construct the expected path to the blob view file
            let base_path = &node.context.config.blobs_base_path;
            let blob_path = base_path.join(blob_view_id.to_string());

            // Read from this file
            let retrieved_blob_data = fs::read(blob_path).await;

            // Number of bytes for the publish and materialized blob should be the same
            assert!(retrieved_blob_data.is_ok());
            assert_eq!(blob_data.len(), retrieved_blob_data.unwrap().len());
        })
    }

    #[rstest]
    fn rejects_incorrect_schema(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish blob
            let blob_data = "Hello, World!";
            let blob_view_id =
                add_blob(&mut node, blob_data.as_bytes(), 5, "plain/text", &key_pair).await;

            // Get the blob document back again.
            let blob_document = node
                .context
                .store
                .get_document_by_view_id(&blob_view_id)
                .await
                .unwrap()
                .unwrap();

            // Get the view id of the first piece of this blob.
            let blob_piece_view_id = match blob_document.get("pieces").unwrap() {
                p2panda_rs::operation::OperationValue::PinnedRelationList(list) => {
                    list.iter().next().unwrap()
                }
                _ => unreachable!(),
            };

            // Run blob task but the input is the document view id of a blob_piece_v1 document.
            let result = blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_piece_view_id.clone()),
            )
            .await;

            // It should fail
            assert!(result.is_err());
        })
    }
}
