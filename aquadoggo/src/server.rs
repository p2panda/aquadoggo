// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

use crate::config::Configuration;
use crate::db::Pool;
use crate::graphql::{
    build_static_schema, handle_graphql_playground, handle_graphql_query, StaticSchema,
};
use crate::rpc::{
    build_rpc_api_service, handle_get_http_request, handle_http_request, RpcApiService,
};
use crate::service_manager::{Context, Sender, Shutdown};

/// Shared state for incoming API requests.
#[derive(Clone)]
pub struct ApiState {
    /// JSON RPC service with RPC handlers.
    // @TODO: This will be removed soon. See: https://github.com/p2panda/aquadoggo/issues/60
    pub rpc_service: RpcApiService,

    /// Database connection pool.
    pub pool: Pool,

    /// Static GraphQL schema.
    pub schema: StaticSchema,

    // Configuration.
    pub config: Configuration,
}

impl ApiState {
    /// Initialize new state with shared connection pool for API requests.
    pub fn new(pool: Pool, config: Configuration) -> Self {
        let rpc_service = build_rpc_api_service(pool.clone());
        let schema = build_static_schema(pool.clone());

        Self {
            rpc_service,
            pool,
            schema,
            config,
        }
    }
}

/// Build HTTP server exposing JSON RPC and GraphQL API.
pub fn build_server(state: ApiState) -> Router {
    // Configure CORS middleware
    let cors = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
        .allow_credentials(false)
        .allow_origin(Any);

    Router::new()
        // Add JSON RPC routes
        // @TODO: The JSON RPC is deprecated and will be replaced soon by GraphQL. See:
        // https://github.com/p2panda/aquadoggo/issues/60
        .route("/", get(handle_get_http_request).post(handle_http_request))
        // Add GraphQL routes
        .route(
            "/graphql",
            get(handle_graphql_playground).post(handle_graphql_query),
        )
        // Add middlewares
        .layer(cors)
        // Add shared state
        .layer(Extension(state))
}

/// Start HTTP server.
pub async fn start_server(Context(state): Context<ApiState>, signal: Shutdown, _tx: Sender<usize>) {
    let http_port = state.config.http_port;
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), http_port);
    let server = build_server(state);

    axum::Server::bind(&http_address)
        .serve(server.into_make_service())
        .with_graceful_shutdown(async {
            signal.await.ok();
        })
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::test_helpers::{initialize_db, TestClient};
    use crate::config::Configuration;

    use super::{build_server, ApiState};

    #[tokio::test]
    async fn rpc_respond_with_method_not_allowed() {
        let config = Configuration::default();
        let pool = initialize_db().await;
        let state = ApiState::new(pool, config);
        let client = TestClient::new(build_server(state));

        let response = client.get("/").send().await;

        assert_eq!(
            response.text().await,
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }

    #[tokio::test]
    async fn graphql_endpoint() {
        let config = Configuration::default();
        let pool = initialize_db().await;
        let state = ApiState::new(pool, config);
        let client = TestClient::new(build_server(state));

        let response = client
            .post("/graphql")
            .json(&json!({
                "query": "{ ping }",
            }))
            .send()
            .await;

        assert_eq!(
            response.text().await,
            json!({
                "data": {
                    "ping": "pong"
                }
            })
            .to_string()
        );
    }
}
