// SPDX-License-Identifier: AGPL-3.0-or-later

use async_std::stream::StreamExt;
use jsonrpc_v2::RequestObject;
use tide_websockets::{Message, WebSocketConnection};

use crate::server::{ApiError, ApiRequest, ApiResult};

/// Handle incoming HTTP JSON RPC requests.
pub async fn handle_http_request(mut request: ApiRequest) -> ApiResult {
    // Parse RPC request
    let rpc_request: RequestObject = request.body_json().await?;

    // Handle RPC request
    let rpc_state = request.state();
    let rpc_result = rpc_state.rpc_service.handle(rpc_request).await;

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
    request: ApiRequest,
    mut stream: WebSocketConnection,
) -> Result<(), ApiError> {
    while let Some(Ok(Message::Text(ws_input))) = stream.next().await {
        // Parse RPC request
        let rpc_request: RequestObject = serde_json::from_str(&ws_input)?;

        // Handle RPC request
        let rpc_state = request.state();
        let rpc_result = rpc_state.rpc_service.handle(rpc_request).await;

        // Serialize response to JSON
        let rpc_result_json = serde_json::to_string(&rpc_result)?;

        // Respond with RPC result
        stream.send_string(rpc_result_json).await?;
    }

    Ok(())
}
