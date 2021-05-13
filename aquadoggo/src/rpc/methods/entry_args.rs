use bamboo_rs_core::entry::is_lipmaa_required;
use jsonrpc_v2::{Data, Params};
use p2panda_rs::atomic::{Hash, Validation};

use crate::db::models::{Entry, Log};
use crate::db::Pool;
use crate::errors::Result;
use crate::rpc::request::EntryArgsRequest;
use crate::rpc::response::EntryArgsResponse;
use crate::rpc::RpcApiState;

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the schemas
/// log_id) to encode a new bamboo entry.
pub async fn get_entry_args(
    data: Data<RpcApiState>,
    Params(params): Params<EntryArgsRequest>,
) -> Result<EntryArgsResponse> {
    // Validate request parameters
    params.author.validate()?;
    params.schema.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Determine log_id for author's schema
    let log_id = Log::find_schema_log_id(&pool, &params.author, &params.schema).await?;

    // Find latest entry in this log
    let entry_latest = Entry::latest(&pool, &params.author, &log_id).await?;

    match entry_latest {
        Some(entry_backlink) => {
            // Determine skiplink ("lipmaa"-link) entry in this log
            let entry_hash_skiplink = determine_skiplink(pool.clone(), &entry_backlink).await?;

            Ok(EntryArgsResponse {
                entry_hash_backlink: Some(entry_backlink.entry_hash),
                entry_hash_skiplink,
                last_seq_num: Some(entry_backlink.seq_num),
                log_id,
            })
        }
        None => Ok(EntryArgsResponse {
            entry_hash_backlink: None,
            entry_hash_skiplink: None,
            last_seq_num: None,
            log_id,
        }),
    }
}

/// Determine skiplink entry hash ("lipmaa"-link) for entry in this log, return `None` when no
/// skiplink is required for the next entry.
pub async fn determine_skiplink(pool: Pool, entry: &Entry) -> Result<Option<Hash>> {
    let next_seq_num = entry.seq_num.clone().next().unwrap();

    // Unwrap as we know that an skiplink exists as soon as previous entry is given
    let skiplink_seq_num = next_seq_num.skiplink_seq_num().unwrap();

    // Check if skiplink is required and return hash if so
    let entry_skiplink_hash = if is_lipmaa_required(next_seq_num.as_i64() as u64) {
        let skiplink_entry =
            Entry::at_seq_num(&pool, &entry.author, &entry.log_id, &skiplink_seq_num)
                .await?
                .unwrap();
        Some(skiplink_entry.entry_hash)
    } else {
        None
    };

    Ok(entry_skiplink_hash)
}

#[cfg(test)]
mod tests {
    use tide_testing::TideTestingExt;

    use crate::rpc::api::rpc_api_handler;
    use crate::rpc::server::build_rpc_server;
    use crate::test_helpers::{
        initialize_db, random_entry_hash, rpc_error, rpc_request, rpc_response,
    };

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

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

    #[async_std::test]
    async fn get_entry_arguments() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let rpc_api_handler = rpc_api_handler(pool);
        let app = build_rpc_server(rpc_api_handler);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "author": "{}",
                    "schema": "{}"
                }}"#,
                TEST_AUTHOR,
                random_entry_hash(),
            ),
        );

        let response = rpc_response(
            r#"{
                "entryHashBacklink": null,
                "entryHashSkiplink": null,
                "lastSeqNum": null,
                "logId": 1
            }"#,
        );

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
