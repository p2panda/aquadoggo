// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use http_types::headers::HeaderValue;
use tide::security::{CorsMiddleware, Origin};
use tide_websockets::WebSocket;

use crate::config::Configuration;
use crate::db::Pool;
use crate::graphql::Endpoint;
use crate::rpc::{build_rpc_api_service, handle_http_request, handle_ws_request, RpcApiService};

pub type ApiServer = tide::Server<ApiState>;
pub type ApiRequest = tide::Request<ApiState>;
pub type ApiResult = tide::Result;
pub type ApiError = tide::Error;

/// Shared state for incoming API requests.
#[derive(Clone)]
pub struct ApiState {
    /// JSON RPC service with RPC handlers
    // @TODO: This will be removed soon. See: https://github.com/p2panda/aquadoggo/issues/60
    pub rpc_service: RpcApiService,

    /// Database connection pool
    pub pool: Pool,
}

impl ApiState {
    /// Initialize new state with shared connection pool for API requests.
    pub fn new(pool: Pool) -> Self {
        let rpc_service = build_rpc_api_service(pool.clone());

        Self { rpc_service, pool }
    }
}

/// Build HTTP and WebSocket server exposing a JSON RPC and GraphQL API.
pub fn build_server(state: ApiState) -> ApiServer {
    // Configure CORS middleware
    let cors = CorsMiddleware::new()
        .allow_methods("GET, POST, OPTIONS".parse::<HeaderValue>().unwrap())
        .allow_origin(Origin::from("*"))
        .allow_credentials(false);

    // Create endpoint for GraphQL requests
    let handle_graphql_request = Endpoint::new(state.pool.clone());

    // Prepare HTTP server and add CORS middleware
    let mut app = tide::with_state(state);
    app.with(cors);

    // Add JSON RPC routes
    // @TODO: The JSON RPC is deprecated and will be removed soon with GraphQL. See:
    // https://github.com/p2panda/aquadoggo/issues/60
    app.at("/")
        .with(WebSocket::new(handle_ws_request))
        .get(|_| async { Ok("Used HTTP Method is not allowed. POST or OPTIONS is required") })
        .post(handle_http_request);

    // Add GraphQL routes
    app.at("/graphql")
        .get(Endpoint::handle_playground_request)
        .post(handle_graphql_request);

    app
}

/// Start HTTP and WebSocket server.
pub async fn start_server(config: &Configuration, state: ApiState) -> anyhow::Result<()> {
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);
    let server = build_server(state);
    server.listen(http_address).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tide_testing::TideTestingExt;

    use crate::server::{build_server, ApiState};
    use crate::test_helpers::initialize_db;

    #[async_std::test]
    async fn respond_with_method_not_allowed() {
        let pool = initialize_db().await;
        let state = ApiState::new(pool.clone());
        let app = build_server(state);

        assert_eq!(
            app.get("/").recv_string().await.unwrap(),
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }
}
