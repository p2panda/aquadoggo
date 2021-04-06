use bamboo_rs_core::entry::is_lipmaa_required;

use crate::db::models::{Entry, Log};
use crate::db::Pool;
use crate::errors::Result;
use crate::rpc::request::EntryArgsRequest;
use crate::rpc::response::EntryArgsResponse;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum EntryArgsError {}

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the schemas
/// log_id) to encode a new bamboo entry.
pub async fn get_entry_args(pool: Pool, params: EntryArgsRequest) -> Result<EntryArgsResponse> {
    // Determine log_id for author's schema
    let log_id = Log::find_schema_log_id(&pool, &params.author, &params.schema).await?;

    // Find latest entry in this log
    let entry_latest = Entry::latest(&pool, &params.author, &log_id).await?;

    match entry_latest {
        Some(entry_backlink) => {
            let next_seq_num = entry_backlink.seq_num.clone().next().unwrap();

            // Unwrap as we know that an skiplink exists as soon as previous entry is given:
            let skiplink_seq_num = next_seq_num.skiplink_seq_num().unwrap();

            // Determine skiplink ("lipmaa"-link) entry in this log, return `None` when no skiplink
            // is required for the next entry
            let entry_hash_skiplink = if is_lipmaa_required(next_seq_num.as_i64() as u64) {
                let entry = Entry::at_seq_num(&pool, &params.author, &log_id, &skiplink_seq_num)
                    .await?
                    .unwrap();
                Some(entry.entry_hash)
            } else {
                None
            };

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

#[cfg(test)]
mod tests {
    use crate::rpc::ApiService;
    use crate::test_helpers::{initialize_db, random_entry_hash, rpc_request, rpc_response};

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

    #[async_std::test]
    async fn get_entry_arguments() {
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

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

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }
}
