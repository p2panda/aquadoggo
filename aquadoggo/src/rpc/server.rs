// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_std::stream::StreamExt;
use http_types::headers::HeaderValue;
use jsonrpc_v2::RequestObject;
use tide::security::{CorsMiddleware, Origin};
use tide::{http::mime, Body, Response, StatusCode};
use tide_websockets::{Message, WebSocket, WebSocketConnection};

use crate::config::Configuration;
use crate::db::Pool;
use crate::graphql::build_schema;
use crate::rpc::RpcApiService;

pub type RpcServer = tide::Server<RpcApiService>;
pub type RpcServerRequest = tide::Request<RpcApiService>;

/// Handle incoming HTTP JSON RPC requests.
pub async fn handle_http_request(mut request: RpcServerRequest) -> tide::Result {
    // Parse RPC request
    let rpc_request: RequestObject = request.body_json().await?;

    // Handle RPC request
    let rpc_server = request.state();
    let rpc_result = rpc_server.handle(rpc_request).await;

    // Serialize response to JSON
    let rpc_result_json = serde_json::to_string(&rpc_result)?;

    // Respond with RPC result
    let response = tide::Response::builder(http_types::StatusCode::Ok)
        .body(rpc_result_json)
        .content_type("application/json-rpc;charset=utf-8")
        .build();

    Ok(response)
}

/// Handle incoming WebSocket JSON RPC requests.
pub async fn handle_ws_request(
    request: RpcServerRequest,
    mut stream: WebSocketConnection,
) -> Result<(), tide::Error> {
    while let Some(Ok(Message::Text(ws_input))) = stream.next().await {
        // Parse RPC request
        let rpc_request: RequestObject = serde_json::from_str(&ws_input)?;

        // Handle RPC request
        let rpc_server = request.state();
        let rpc_result = rpc_server.handle(rpc_request).await;

        // Serialize response to JSON
        let rpc_result_json = serde_json::to_string(&rpc_result)?;

        // Respond with RPC result
        stream.send_string(rpc_result_json).await?;
    }

    Ok(())
}

/// Build HTTP and WebSocket server both exposing a JSON RPC API.
pub fn build_rpc_server(api: RpcApiService, pool: Pool) -> RpcServer {
    // Configure CORS middleware
    let cors = CorsMiddleware::new()
        .allow_methods("GET, POST, OPTIONS".parse::<HeaderValue>().unwrap())
        .allow_origin(Origin::from("*"))
        .allow_credentials(false);

    // Prepare HTTP server with RPC route
    let mut app = tide::with_state(api);
    app.with(cors);
    app.at("/")
        .with(WebSocket::new(handle_ws_request))
        .get(|_| async { Ok("Used HTTP Method is not allowed. POST or OPTIONS is required") })
        .post(handle_http_request);

    // Add GraphQL routes
    app.at("/graphql")
        .get(|_| async move {
            let mut resp = Response::new(StatusCode::Ok);
            resp.set_body(Body::from_string(playground_source(
                GraphQLPlaygroundConfig::new("/graphql"),
            )));
            resp.set_content_type(mime::HTML);
            Ok(resp)
        })
        .post(async_graphql_tide::graphql(build_schema(pool)));
    app
}

/// Start HTTP and WebSocket server.
pub async fn start_rpc_server(
    config: &Configuration,
    api: RpcApiService,
    pool: Pool,
) -> anyhow::Result<()> {
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);
    let server = build_rpc_server(api, pool);
    server.listen(http_address).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tide_testing::TideTestingExt;

    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::initialize_db;

    #[async_std::test]
    async fn respond_with_method_not_allowed() {
        let pool = initialize_db().await;
        let rpc_api = build_rpc_api_service(pool.clone());
        let app = build_rpc_server(rpc_api, pool);

        assert_eq!(
            app.get("/").recv_string().await.unwrap(),
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }
}
