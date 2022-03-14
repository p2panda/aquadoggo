// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::sql_storage::SqlStorage;
use crate::errors::Result;
use crate::rpc::request::EntryArgsRequest;
use crate::rpc::response::EntryArgsResponse;

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the
/// document's log_id) to encode a new bamboo entry.
pub async fn get_entry_args(
    storage_provider: Data<SqlStorage>,
    Params(params): Params<EntryArgsRequest>,
) -> Result<EntryArgsResponse> {
    let response = storage_provider
        .get_entry_args(&params.author, params.document.as_ref())
        .await
        .unwrap();
    Ok(response)
}

#[cfg(test)]
mod tests {
    use crate::rpc::api::build_rpc_api_service;
    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::{
        handle_http, initialize_db, random_entry_hash, rpc_error, rpc_request, rpc_response,
    };

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

    #[async_std::test]
    async fn respond_with_wrong_author_error() {
        let pool = initialize_db().await;
        let rpc_api = build_rpc_api_service(pool.clone());
        let app = build_rpc_server(rpc_api);

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

        assert_eq!(handle_http(&app, request).await, response);
    }

    #[async_std::test]
    async fn get_entry_arguments() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let rpc_api = build_rpc_api_service(pool);
        let app = build_rpc_server(rpc_api);

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
                "logId": "1",
                "seqNum": "1"
            }"#,
        );

        assert_eq!(handle_http(&app, request).await, response);
    }
}
