// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Result;
use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

use crate::context::Context;
use crate::graphql::{handle_graphql_playground, handle_graphql_query};
use crate::rpc::{handle_get_http_request, handle_http_request};
use crate::service_manager::Shutdown;
use crate::service_message::ServiceSender;

const GRAPHQL_ROUTE: &str = "/graphql";

/// Build HTTP server with GraphQL API.
pub fn build_server(context: Context) -> Router {
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
            GRAPHQL_ROUTE,
            get(|| handle_graphql_playground(GRAPHQL_ROUTE)).post(handle_graphql_query),
        )
        // Add middlewares
        .layer(cors)
        // Add shared context
        .layer(Extension(context))
}

/// Start HTTP server.
pub async fn http_service(context: Context, signal: Shutdown, _tx: ServiceSender) -> Result<()> {
    let http_port = context.config.http_port;
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), http_port);

    axum::Server::try_bind(&http_address)?
        .serve(build_server(context).into_make_service())
        .with_graceful_shutdown(async {
            signal.await.ok();
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::test_helpers::{initialize_db, TestClient};

    use super::build_server;

    #[tokio::test]
    async fn rpc_respond_with_method_not_allowed() {
        let pool = initialize_db().await;
        let context = Context::new(pool, Configuration::default());
        let client = TestClient::new(build_server(context));

        let response = client.get("/").send().await;

        assert_eq!(
            response.text().await,
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }

    #[tokio::test]
    async fn graphql_endpoint() {
        let pool = initialize_db().await;
        let context = Context::new(pool, Configuration::default());
        let client = TestClient::new(build_server(context));

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
