use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use http_types::headers::HeaderValue;
use jsonrpc_v2::RequestObject;
use tide::security::{CorsMiddleware, Origin};

use crate::config::Configuration;
use crate::rpc::RpcApiService;
use crate::task::TaskManager;

/// Manages both HTTP and WebSocket server to expose the JSON RPC API.
pub struct RpcServer {}

impl RpcServer {
    /// Spawn two separate tasks running HTTP and WebSocket servers both exposing a JSON RPC API.
    pub fn start(
        config: &Configuration,
        task_manager: &mut TaskManager,
        api: RpcApiService,
    ) -> Self {
        let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);

        // Configure CORS middleware
        let cors = CorsMiddleware::new()
            .allow_methods("GET, POST, OPTIONS".parse::<HeaderValue>().unwrap())
            .allow_origin(Origin::from("*"))
            .allow_credentials(false);

        // Prepare HTTP server with RPC route
        let mut app = tide::with_state(api);
        app.with(cors);
        app.at("/").get(|_| async {
            Ok("Used HTTP Method is not allowed. POST or OPTIONS is required")
        });
        app.at("/").post(RpcServer::handle_http_request);

        // Start server
        task_manager.spawn("RPC Server", async move {
            app.listen(http_address).await?;
            Ok(())
        });

        Self {}
    }

    pub async fn handle_http_request(mut request: tide::Request<RpcApiService>) -> tide::Result {
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
}
