// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::db::provider::SqlStorage;
use crate::errors::StorageProviderResult;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QueryEntriesResponse;

pub async fn query_entries(
    storage_provider: Data<SqlStorage>,
    Params(params): Params<QueryEntriesRequest>,
) -> StorageProviderResult<QueryEntriesResponse> {
    // Find and return raw entries from database
    let entries = storage_provider.by_schema(&params.schema).await?;

    Ok(QueryEntriesResponse { entries })
}

#[cfg(test)]
mod tests {
    use p2panda_rs::hash::Hash;
    use p2panda_rs::schema::SchemaId;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::server::build_server;
    use crate::test_helpers::{handle_http, initialize_db, rpc_request, rpc_response, TestClient};

    #[tokio::test]
    async fn query_entries() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let context = Context::new(pool, Configuration::default());
        let app = build_server(context);
        let client = TestClient::new(app);

        // Prepare request to API
        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );

        let request = rpc_request(
            "panda_queryEntries",
            &format!(
                r#"{{
                    "schema": "{}"
                }}"#,
                schema.as_str(),
            ),
        );

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        assert_eq!(handle_http(&client, request).await, response);
    }
}
