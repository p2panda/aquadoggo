// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::{Json, Router};
use jsonrpc_v2::{RequestObject, ResponseObjects};
use tower_http::cors::{Any, CorsLayer};

use crate::config::Configuration;
use crate::rpc::RpcApiService;

/// Handle incoming HTTP JSON RPC requests.
async fn handle_http_request(
    Json(rpc_request): Json<RequestObject>,
    Extension(rpc_server): Extension<RpcApiService>,
) -> Json<ResponseObjects> {
    let response = rpc_server.handle(rpc_request).await;
    Json(response)
}

/// Handle RPC requests with wrong HTTP method.
async fn handle_get_http_request() -> &'static str {
    "Used HTTP Method is not allowed. POST or OPTIONS is required"
}

/// Build HTTP server exposing a JSON RPC API.
pub fn build_rpc_server(api: RpcApiService) -> Router {
    // Configure CORS middleware
    let cors = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
        .allow_credentials(false)
        .allow_origin(Any);

    // Prepare HTTP server with RPC route
    let app = Router::new()
        .route("/", get(handle_get_http_request).post(handle_http_request))
        .layer(cors)
        .layer(Extension(api));
    app
}

/// Start HTTP server.
pub async fn start_rpc_server(config: &Configuration, api: RpcApiService) -> anyhow::Result<()> {
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);
    let server = build_rpc_server(api);
    axum::Server::bind(&http_address)
        .serve(server.into_make_service())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tower::ServiceExt;

    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::initialize_db;

    #[tokio::test]
    async fn respond_with_method_not_allowed() {
        let pool = initialize_db().await;
        let rpc_api = build_rpc_api_service(pool.clone());
        let app = build_rpc_server(rpc_api);

        let client = TestClient::new(app);

        assert_eq!(
            app.get("/").recv_string().await.unwrap(),
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }

    #[tokio::test]
    async fn nest_at_capture() {
        let api_routes = Router::new().route("/:b", get(|| async {})).boxed_clone();

        let app = Router::new().nest("/:a", api_routes);

        let client = TestClient::new(app);

        let res = client.get("/foo/bar").send().await;
        assert_eq!(res.status(), StatusCode::OK);
    }
}
