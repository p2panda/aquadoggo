// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::store::SqlStorage;
use crate::errors::StorageProviderResult;
use crate::rpc::request::EntryArgsRequest;
use crate::rpc::response::EntryArgsResponse;

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the
/// document's log_id) to encode a new bamboo entry.
pub async fn get_entry_args(
    storage_provider: Data<SqlStorage>,
    Params(params): Params<EntryArgsRequest>,
) -> StorageProviderResult<EntryArgsResponse> {
    let response = storage_provider.get_entry_args(&params).await?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use crate::config::Configuration;
    use crate::server::{build_server, ApiState};
    use crate::test_helpers::{
        handle_http, initialize_db, random_entry_hash, rpc_error, rpc_request, rpc_response,
        TestClient,
    };

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

    #[tokio::test]
    async fn respond_with_wrong_author_error() {
        let config = Configuration::default();
        let pool = initialize_db().await;
        let state = ApiState::new(pool, config);
        let app = build_server(state);
        let client = TestClient::new(app);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "author": "1234",
                    "document": "{}"
                }}"#,
                random_entry_hash()
            ),
        );

        let response = rpc_error("invalid author key length");
        assert_eq!(handle_http(&client, request).await, response);
    }

    #[tokio::test]
    async fn get_entry_arguments() {
        let config = Configuration::default();
        let pool = initialize_db().await;
        let state = ApiState::new(pool, config);
        let app = build_server(state);
        let client = TestClient::new(app);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "author": "{}",
                    "document": null
                }}"#,
                TEST_AUTHOR,
            ),
        );

        let response = rpc_response(
            r#"{
                "entryHashBacklink": null,
                "entryHashSkiplink": null,
                "seqNum": "1",
                "logId": "1"
            }"#,
        );

        assert_eq!(handle_http(&client, request).await, response);
    }
}
