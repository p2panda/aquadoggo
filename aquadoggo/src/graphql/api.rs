// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use tide::{http::mime, Body, Response, StatusCode};

use crate::db::Pool;
use crate::graphql::convert::gql_to_sql;
use crate::graphql::QueryRoot;
use crate::server::{ApiRequest, ApiResult, ApiState};

/// GraphQL Endpoint from `async_graphql` crate handling statically defined GraphQL queries.
type StaticEndpoint =
    async_graphql_tide::GraphQLEndpoint<QueryRoot, EmptyMutation, EmptySubscription>;

/// Statically defined GraphQL schema.
pub type StaticSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Returns statically defined GraphQL schema.
fn build_static_schema(pool: Pool) -> StaticSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}

/// Endpoint for all incoming GraphQL queries.
pub struct Endpoint {
    static_endpoint: StaticEndpoint,
}

impl Endpoint {
    /// Returns a new endpoint instance.
    pub fn new(pool: Pool) -> Self {
        let schema = build_static_schema(pool);

        Self {
            static_endpoint: async_graphql_tide::graphql(schema),
        }
    }

    /// Static handler for serving the HTML page of the GraphQL playground.
    pub async fn handle_playground_request(_request: ApiRequest) -> ApiResult {
        let mut response = Response::new(StatusCode::Ok);

        response.set_body(Body::from_string(playground_source(
            GraphQLPlaygroundConfig::new("/graphql"),
        )));
        response.set_content_type(mime::HTML);

        Ok(response)
    }
}

#[tide::utils::async_trait]
impl tide::Endpoint<ApiState> for Endpoint {
    /// Tide handler for all GraphQL requests.
    ///
    /// This handler resolves all incoming GraphQL requests and answers them both for dynamically
    /// and statically generated schemas.
    async fn call(&self, mut request: ApiRequest) -> ApiResult {
        let graphql_request = request.body_json::<async_graphql::Request>().await?;
        let sql = gql_to_sql(&graphql_request.query).unwrap();
        println!("{:?}", sql);

        // @TODO: Implement handling of dynamic GraphQL requests. Route all GraphQL requests to
        // static handler for now.
        self.static_endpoint.call(request).await
    }
}
