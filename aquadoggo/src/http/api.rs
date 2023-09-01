// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Extension, Path};
use axum::response::{self, IntoResponse};
use http::StatusCode;
use p2panda_rs::document::{DocumentId, DocumentViewHash};

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

pub async fn handle_blob_document(
    Extension(context): Extension<HttpServiceContext>,
    Path(document_id): Path<DocumentId>,
) -> Result<String, StatusCode> {
    Ok("Hello".into())
}

pub async fn handle_blob_view(
    Extension(context): Extension<HttpServiceContext>,
    // @TODO: Deserialize needs to be implemented for `DocumentViewHash` so we can use it here in
    // combination with `Path`. Related issue: https://github.com/p2panda/p2panda/issues/514
    Path((document_id, view_hash)): Path<(DocumentId, String)>,
) -> Result<String, StatusCode> {
    let view_hash =
        DocumentViewHash::new(view_hash.try_into().map_err(|_| StatusCode::BAD_REQUEST)?);
    Ok("Hello".into())
}
