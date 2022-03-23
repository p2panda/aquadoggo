// SPDX-License-Identifier: AGPL-3.0-or-later

use axum::extract::Extension;
use axum::Json;
use jsonrpc_v2::{RequestObject, ResponseObjects};

use crate::server::ApiState;

/// Handle incoming HTTP JSON RPC requests.
pub async fn handle_http_request(
    Json(rpc_request): Json<RequestObject>,
    Extension(state): Extension<ApiState>,
) -> Json<ResponseObjects> {
    let response = state.rpc_service.handle(rpc_request).await;
    Json(response)
}

/// Handle RPC requests with wrong HTTP method.
pub async fn handle_get_http_request() -> &'static str {
    "Used HTTP Method is not allowed. POST or OPTIONS is required"
}
