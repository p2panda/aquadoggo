// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Result;
use axum::extract::Extension;
use axum::http::Method;
use axum::routing::get;
use axum::Router;
use log::{debug, warn};
use tower_http::cors::{Any, CorsLayer};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::http::api::{handle_graphql_playground, handle_graphql_query};
use crate::http::context::HttpServiceContext;
use crate::manager::{ServiceReadySender, Shutdown};

const GRAPHQL_ROUTE: &str = "/graphql";

/// Build HTTP server with GraphQL API.
pub fn build_server(http_context: HttpServiceContext) -> Router {
    // Configure CORS middleware
    let cors = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
        .allow_credentials(false)
        .allow_origin(Any);

    Router::new()
        // Add GraphQL routes
        .route(
            GRAPHQL_ROUTE,
            get(|| handle_graphql_playground(GRAPHQL_ROUTE)).post(handle_graphql_query),
        )
        // Add middlewares
        .layer(cors)
        // Add shared context
        .layer(Extension(http_context))
}

/// Start HTTP server.
pub async fn http_service(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let http_port = context.config.http_port;
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), http_port);

    // Introduce a new context for all HTTP routes
    let http_context = HttpServiceContext::new(context.store.clone(), tx);

    axum::Server::try_bind(&http_address)?
        .serve(build_server(http_context).into_make_service())
        .with_graceful_shutdown(async {
            debug!("HTTP service is ready");
            if tx_ready.send(()).is_err() {
                warn!("No subscriber informed about HTTP service being ready");
            };

            signal.await.ok();
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::http::context::HttpServiceContext;
    use crate::test_helpers::TestClient;

    use super::build_server;

    #[rstest]
    fn graphql_endpoint(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let (tx, _) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
            let client = TestClient::new(build_server(context));

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": "{ __schema { __typename } }",
                }))
                .send()
                .await;

            assert_eq!(
                response.text().await,
                json!({
                    "data": {
                        "__schema": {
                            "__typename": "__Schema"
                        }
                    }
                })
                .to_string()
            );
        })
    }
}
