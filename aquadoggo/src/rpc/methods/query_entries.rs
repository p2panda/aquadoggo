// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::Validate;

use crate::db::models::Entry;
use crate::errors::Result;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QuerytEntriesResponse;
use crate::rpc::RpcApiState;

pub async fn query_entries(
    data: Data<RpcApiState>,
    Params(params): Params<QueryEntriesRequest>,
) -> Result<QuerytEntriesResponse> {
    // Validate request parameters
    params.document.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Find and return raw entries from database
    let entries = Entry::by_document(&pool, &params.document).await?;
    Ok(QuerytEntriesResponse { entries })
}

#[cfg(test)]
mod tests {
    use p2panda_rs::hash::Hash;

    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::{handle_http, initialize_db, rpc_request, rpc_response};

    #[async_std::test]
    async fn query_document_entries() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let rpc_api = build_rpc_api_service(pool);
        let app = build_rpc_server(rpc_api);

        // Prepare request to API
        let document = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let request = rpc_request(
            "panda_queryEntries",
            &format!(
                r#"{{
                    "document": "{}"
                }}"#,
                document.as_str(),
            ),
        );

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        assert_eq!(handle_http(&app, request).await, response);
    }
}
