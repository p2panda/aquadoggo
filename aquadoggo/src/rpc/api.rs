use std::sync::Arc;

use jsonrpc_v2::{Data, MapRouter, Server as Handler};

use crate::db::Pool;
use crate::rpc::methods::{get_entry_args, publish_entry, query_entries};

pub type RpcApiService = Arc<Handler<MapRouter>>;

#[derive(Debug, Clone)]
pub struct RpcApiState {
    pub pool: Pool,
}

pub fn rpc_api_handler(pool: Pool) -> RpcApiService {
    let state = RpcApiState { pool };

    Handler::new()
        .with_data(Data(Arc::new(state)))
        .with_method("panda_getEntryArguments", get_entry_args)
        .with_method("panda_publishEntry", publish_entry)
        .with_method("panda_queryEntries", query_entries)
        .finish()
}

#[cfg(test)]
mod tests {
    use tide_testing::TideTestingExt;

    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::{initialize_db, random_entry_hash, rpc_error, rpc_request};

    use super::rpc_api_handler;

    #[async_std::test]
    async fn respond_with_method_not_allowed() {
        let pool = initialize_db().await;
        let rpc_api_handler = rpc_api_handler(pool.clone());
        let app = build_rpc_server(rpc_api_handler);

        assert_eq!(
            app.get("/").recv_string().await.unwrap(),
            "Used HTTP Method is not allowed. POST or OPTIONS is required"
        );
    }

    #[async_std::test]
    async fn respond_with_wrong_author_error() {
        let pool = initialize_db().await;
        let rpc_api_handler = rpc_api_handler(pool.clone());
        let app = build_rpc_server(rpc_api_handler);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "author": "1234",
                    "schema": "{}"
                }}"#,
                random_entry_hash()
            ),
        );

        let response = rpc_error("invalid author key length");

        let response_body: serde_json::value::Value = app
            .post("/")
            .body(tide::Body::from_string(request.into()))
            .content_type("application/json")
            .recv_json()
            .await
            .unwrap();

        assert_eq!(response_body.to_string(), response);
    }
}
