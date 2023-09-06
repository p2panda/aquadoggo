// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::body::StreamBody;
use axum::extract::{Extension, Path};
use axum::headers::{ETag, IfNoneMatch};
use axum::http::StatusCode;
use axum::response::{self, IntoResponse, Response};
use axum::TypedHeader;
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
    TypedHeader(if_none_match): TypedHeader<IfNoneMatch>,
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

    respond_with_blob(if_none_match, context.blob_dir_path, document).await
}

/// Handle requests for a blob document view served via HTTP.
///
/// This method returns the version which was specified by the document view id.
pub async fn handle_blob_view(
    TypedHeader(if_none_match): TypedHeader<IfNoneMatch>,
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

    respond_with_blob(if_none_match, context.blob_dir_path, document).await
}

/// Returns HTTP response with the contents, ETag and given MIME type of a blob.
///
/// Supports basic caching by handling "IfNoneMatch" headers matching the latest ETag.
async fn respond_with_blob(
    if_none_match: IfNoneMatch,
    blob_dir_path: PathBuf,
    document: impl AsDocument,
) -> Result<Response, BlobHttpError> {
    let view_id = document.view_id();

    // Convert document view id into correct ETag value (with quotation marks defined in
    // https://datatracker.ietf.org/doc/html/rfc7232#section-2.3)
    let to_etag_str = || format!("\"{}\"", view_id);

    // Respond with 304 "not modified" if ETag still matches (document did not get updated)
    let etag =
        ETag::from_str(&to_etag_str()).map_err(|err| BlobHttpError::InternalError(err.into()))?;
    if !if_none_match.precondition_passes(&etag) {
        return Ok(StatusCode::NOT_MODIFIED.into_response());
    }

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
            let headers = [
                // MIME type to allow browsers to correctly handle this specific blob format
                (header::CONTENT_TYPE, mime_type_str),
                // ETag to allow browsers handle caching
                (header::ETAG, &to_etag_str()),
            ];

            let stream = ReaderStream::new(file);
            let body = StreamBody::new(stream);

            Ok((headers, body).into_response())
        }
        Err(_) => {
            warn!(
                "Data inconsistency detected: Blob document {} exists in database but not on file
                system at path {}!",
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
    use http::{header, StatusCode};
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::validate::MAX_BLOB_PIECE_LENGTH;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::materializer::tasks::blob_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{add_blob, http_test_client, test_runner, update_blob, TestNode};

    #[rstest]
    fn responds_with_blob_in_http_body(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await
            .unwrap();

            let client = http_test_client(&node).await;

            // "/blobs/<document_id>" path
            let response = client.get(&format!("/blobs/{}", document_id)).send().await;
            let status_code = response.status();
            let body = response.text().await;

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, World!");

            // "/blobs/<document_id>/<view_id>" path
            let response = client
                .get(&format!("/blobs/{}/{}", document_id, blob_view_id))
                .send()
                .await;
            let status_code = response.status();
            let body = response.text().await;

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, World!");
        })
    }

    #[rstest]
    fn document_route_responds_with_latest_view(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await
            .unwrap();

            // Update the blob
            let blob_data = "Hello, Panda!".as_bytes();
            let blob_view_id_2 =
                update_blob(&mut node, &blob_data, 6, &blob_view_id, &key_pair).await;

            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id_2.clone()),
            )
            .await
            .unwrap();

            // Expect to receive latest version
            let client = http_test_client(&node).await;
            let response = client.get(&format!("/blobs/{}", document_id)).send().await;
            let status_code = response.status();
            let body = response.text().await;

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, Panda!");
        })
    }

    #[rstest]
    fn responds_with_content_type_header(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = r#"
                <svg version="1.1" width="300" height="200" xmlns="http://www.w3.org/2000/svg">
                  <rect width="100%" height="100%" fill="red" />
                  <circle cx="150" cy="100" r="80" fill="green" />
                </svg>
            "#
            .as_bytes();
            let blob_view_id = add_blob(
                &mut node,
                &blob_data,
                MAX_BLOB_PIECE_LENGTH,
                "image/svg+xml",
                &key_pair,
            )
            .await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await
            .unwrap();

            // Expect correctly set content type header and body in response
            let client = http_test_client(&node).await;
            let response = client.get(&format!("/blobs/{}", document_id)).send().await;
            let status_code = response.status();
            let headers = response.headers();
            let body = response.bytes().await;
            let content_type = headers
                .get(header::CONTENT_TYPE)
                .expect("ContentType to exist in header");

            assert_eq!(content_type, "image/svg+xml");
            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, blob_data);
        })
    }

    #[rstest]
    fn handles_etag_and_if_none_match_precondition(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id.clone()),
            )
            .await
            .unwrap();

            let client = http_test_client(&node).await;

            // 1. Get blob and ETag connected to it
            let response = client.get(&format!("/blobs/{}", document_id)).send().await;
            let status_code = response.status();
            let headers = response.headers();
            let body = response.text().await;
            let etag = headers.get(header::ETAG).expect("ETag to exist in header");

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, World!");

            // 2. Send another request, including the received ETag inside a "IfNoneMatch" header
            let response = client
                .get(&format!("/blobs/{}", document_id))
                .header(header::IF_NONE_MATCH, etag)
                .send()
                .await;
            let status_code = response.status();
            let body = response.text().await;
            assert_eq!(status_code, StatusCode::NOT_MODIFIED);
            assert_eq!(body, "");

            // 3. Update the blob
            let blob_data = "Hello, Panda!".as_bytes();
            let blob_view_id_2 =
                update_blob(&mut node, &blob_data, 6, &blob_view_id, &key_pair).await;

            // Make sure to materialize blob on file system
            blob_task(
                node.context.clone(),
                TaskInput::DocumentViewId(blob_view_id_2.clone()),
            )
            .await
            .unwrap();

            // 4. Send request again, including the (now outdated) ETag
            let response = client
                .get(&format!("/blobs/{}", document_id))
                .header(header::IF_NONE_MATCH, etag)
                .send()
                .await;
            let status_code = response.status();
            let headers = response.headers();
            let body = response.text().await;
            let etag_2 = headers.get(header::ETAG).expect("ETag to exist in header");

            assert_ne!(etag, etag_2);
            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body, "Hello, Panda!");
        })
    }

    #[rstest]
    #[case::inexisting_document_id(
        "/blobs/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        StatusCode::NOT_FOUND
    )]
    #[case::inexisting_document_view_id(
        "/blobs/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        StatusCode::NOT_FOUND
    )]
    #[case::invalid_document_id("/blobs/not_valid", StatusCode::BAD_REQUEST)]
    #[case::invalid_document_view_id(
        "/blobs/0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/not_valid",
        StatusCode::BAD_REQUEST
    )]
    fn error_responses(#[case] path: &'static str, #[case] expected_status_code: StatusCode) {
        test_runner(move |node: TestNode| async move {
            let client = http_test_client(&node).await;
            let response = client.get(path).send().await;
            assert_eq!(response.status(), expected_status_code);
        })
    }
}
