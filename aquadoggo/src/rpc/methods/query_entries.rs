use jsonrpc_v2::{Data, Params};
use p2panda_rs::atomic::Validation;

use crate::db::models::Entry;
use crate::errors::Result;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QueryEntriesResponse;
use crate::rpc::RpcApiState;

pub async fn query_entries(
    data: Data<RpcApiState>,
    Params(params): Params<QueryEntriesRequest>,
) -> Result<QueryEntriesResponse> {
    // Validate request parameters
    params.schema.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Find and return raw entries from database
    let entries = Entry::by_schema(&pool, &params.schema).await?;
    Ok(QueryEntriesResponse { entries })
}

#[cfg(test)]
mod tests {
    use tide_testing::TideTestingExt;

    use crate::rpc::api::rpc_api_handler;
    use crate::rpc::server::handle_http_request;
    use crate::test_helpers::{initialize_db, rpc_request, rpc_response};
    use p2panda_rs::atomic::Hash;

    #[async_std::test]
    async fn query_entries() {
        // Prepare test database
        let pool = initialize_db().await;
        let rpc_api_handler = rpc_api_handler(pool);
       
        // Create tider server with endpoints
        let mut app = tide::with_state(rpc_api_handler);
        app.at("/")
            .get(|_| async { Ok("Used HTTP Method is not allowed. POST or OPTIONS is required") })
            .post(handle_http_request);
  
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

        // Prepare request to API
        let request = rpc_request(
            "panda_queryEntries",
            &format!(
                r#"{{
                    "schema": "{}"
                }}"#,
                schema.as_hex(),
            ),
        );

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        let response_body: serde_json::value::Value = app
            .post("/")
            .body(tide::Body::from_string(request.into()))
            .content_type("application/json")
            .recv_json()
            .await.unwrap();

        assert_eq!(response_body.to_string(), response);
    }
}
