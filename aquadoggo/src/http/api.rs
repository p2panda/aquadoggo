// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::body::StreamBody;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::response::{self, IntoResponse, Response};
use http::header;
use log::warn;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::Human;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::http::context::HttpServiceContext;

/// Handle GraphQL playground requests at the given path.
pub async fn handle_graphql_playground(path: &str) -> impl IntoResponse {
    response::Html(playground_source(GraphQLPlaygroundConfig::new(path)))
}

/// Handle GraphQL requests.
pub async fn handle_graphql_query(
    Extension(context): Extension<HttpServiceContext>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    context.schema.execute(req.into_inner()).await.into()
}

/// Handle requests for a blob document served via HTTP.
///
/// This method automatically returns the "latest" version of the document.
pub async fn handle_blob_document(
    Extension(context): Extension<HttpServiceContext>,
    Path(document_id): Path<String>,
) -> Result<Response, BlobHttpError> {
    let document_id: DocumentId = DocumentId::from_str(&document_id)
        .map_err(|err| BlobHttpError::InvalidFormat(err.into()))?;

    let document = context
        .store
        .get_document(&document_id)
        .await
        .map_err(|err| BlobHttpError::InternalError(err.into()))?
        .ok_or_else(|| BlobHttpError::NotFound)?;

    // Requested document is not a blob, treat this as a "not found" error
    if document.schema_id() != &SchemaId::Blob(1) {
        return Err(BlobHttpError::NotFound);
    }

    respond_with_blob(context.blob_dir_path, document).await
}

/// Handle requests for a blob document view served via HTTP.
///
/// This method returns the version which was specified by the document view id.
pub async fn handle_blob_view(
    Extension(context): Extension<HttpServiceContext>,
    Path((document_id, view_id)): Path<(String, String)>,
) -> Result<Response, BlobHttpError> {
    let document_id = DocumentId::from_str(&document_id)
        .map_err(|err| BlobHttpError::InvalidFormat(err.into()))?;
    let view_id = DocumentViewId::from_str(&view_id)
        .map_err(|err| BlobHttpError::InvalidFormat(err.into()))?;

    let document = context
        .store
        .get_document_by_view_id(&view_id)
        .await
        .map_err(|err| BlobHttpError::InternalError(err.into()))?
        .ok_or(BlobHttpError::NotFound)?;

    if document.id() != &document_id || document.schema_id() != &SchemaId::Blob(1) {
        return Err(BlobHttpError::NotFound);
    }

    respond_with_blob(context.blob_dir_path, document).await
}

/// Returns HTTP response with the contents and given MIME type of a blob.
async fn respond_with_blob(
    blob_dir_path: PathBuf,
    document: impl AsDocument,
) -> Result<Response, BlobHttpError> {
    let view_id = document.view_id();

    // Get MIME type of blob
    let mime_type_str = match document.get("mime_type") {
        Some(p2panda_rs::operation::OperationValue::String(value)) => Ok(value),
        _ => Err(BlobHttpError::InternalError(anyhow!(
            "Blob document did not contain a valid 'mime_type' field"
        ))),
    }?;

    // Get body from read-stream of stored file on file system
    let mut file_path = blob_dir_path;
    file_path.push(format!("{view_id}"));
    match File::open(&file_path).await {
        Ok(file) => {
            let stream = ReaderStream::new(file);
            let body = StreamBody::new(stream);
            let headers = [(header::CONTENT_TYPE, mime_type_str)];
            Ok((headers, body).into_response())
        }
        Err(_) => {
            warn!(
                "Data inconsistency detected: Blob document {} exists in database but not on file system at path {}!",
                view_id.display(),
                file_path.display()
            );

            Err(BlobHttpError::NotFound)
        }
    }
}

#[derive(Debug)]
pub enum BlobHttpError {
    NotFound,
    InvalidFormat(anyhow::Error),
    InternalError(anyhow::Error),
}

impl IntoResponse for BlobHttpError {
    fn into_response(self) -> Response {
        match self {
            BlobHttpError::NotFound => {
                (StatusCode::NOT_FOUND, "Could not find document").into_response()
            }
            BlobHttpError::InvalidFormat(err) => (
                StatusCode::BAD_REQUEST,
                format!("Could not parse identifier: {}", err),
            )
                .into_response(),
            BlobHttpError::InternalError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", err),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use p2panda_rs::{document::DocumentId, identity::KeyPair};
    use rstest::rstest;

    use crate::materializer::tasks::blob_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{add_blob, http_test_client, test_runner, TestNode};

    #[rstest]
    fn responds_with_latest_blob_view(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".to_string();
            let blob_view_id = add_blob(&mut node, &blob_data, &key_pair).await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await
            .unwrap();

            let client = http_test_client(&node).await;
            let response = client.get(&format!("/blobs/{}", document_id)).send().await;
            let status_code = response.status();
            let body = response.text().await;

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, World!");
        })
    }

    #[test]
    fn not_found_error() {
        test_runner(|node: TestNode| async move {
            let client = http_test_client(&node).await;

            // Document id not found
            let response = client
                .get(&format!(
                    "/blobs/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ))
                .send()
                .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            // Document view id not found
            let response = client
                .get(&format!(
                    "/blobs/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ))
                .send()
                .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        })
    }
}
