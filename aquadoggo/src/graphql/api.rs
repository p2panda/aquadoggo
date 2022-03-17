// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::Extension;
use axum::response::{self, IntoResponse};

use crate::server::ApiState;

pub async fn handle_graphql_playground() -> impl IntoResponse {
    response::Html(playground_source(GraphQLPlaygroundConfig::new("/")))
}

pub async fn handle_graphql_query(
    request: GraphQLRequest,
    Extension(state): Extension<ApiState>,
) -> GraphQLResponse {
    println!("{:?}", request.0);
    state.schema.execute(request.into_inner()).await.into()
}
