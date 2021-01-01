use async_std::channel::{unbounded, Sender};
use async_std::task;
use jsonrpc_core::{BoxFuture, IoHandler, Params};
use jsonrpc_derive::rpc;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::db::models::{Entry, Log};
use crate::db::Pool;
use crate::errors::Result;
use crate::types::{Author, EntryHash, LogId, Schema, SeqNum};

/// Request body of `panda_getEntryArguments`.
#[derive(Deserialize, Validate, Debug)]
pub struct EntryArgsRequest {
    #[validate]
    author: Author,
    #[validate]
    schema: Schema,
}

/// Response body of `panda_getEntryArguments`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    entry_hash_backlink: Option<EntryHash>,
    entry_hash_skiplink: Option<EntryHash>,
    last_seq_num: Option<SeqNum>,
    log_id: LogId,
}

/// Trait defining all Node RPC API methods.
#[rpc(server)]
pub trait Api {
    #[rpc(name = "panda_getEntryArguments", params = "raw")]
    fn get_entry_args(&self, params: Params) -> BoxFuture<Result<EntryArgsResponse>>;
}

/// Channel messages to send RPC command requests and their payloads from frontend `Api` to
/// `ApiService` backend.
///
/// Every message contains a `Sender` as a `back_channel` to send the response back to the RPC
/// frontend.
#[derive(Debug)]
enum ApiServiceMessages {
    GetEntryArgs(EntryArgsRequest, Sender<Result<EntryArgsResponse>>),
}

/// Backend service handling the RPC API methods.
pub struct ApiService {
    service_channel: Sender<ApiServiceMessages>,
}

impl ApiService {
    /// Creates a JSON RPC API service.
    pub fn new(pool: Pool) -> Self {
        let (service_channel, service_channel_notifier) = unbounded::<ApiServiceMessages>();

        task::spawn(async move {
            while !service_channel_notifier.is_closed() {
                let pool = pool.clone();

                match service_channel_notifier.recv().await {
                    Ok(ApiServiceMessages::GetEntryArgs(params, back_channel)) => {
                        back_channel
                            .send(get_entry_args(pool, params).await)
                            .await
                            .unwrap();
                    }
                    Err(_err) => {
                        // Channel closed, there are no more messages.
                    }
                }
            }
        });

        ApiService { service_channel }
    }

    /// Creates JSON RPC API service and wraps it around a jsonrpc_core `IoHandler` object which
    /// can be used for a server exposing the API.
    pub fn io_handler(pool: Pool) -> IoHandler {
        let mut io = IoHandler::default();
        io.extend_with(Api::to_delegate(ApiService::new(pool)));
        io
    }
}

/// RPC API frontend for `ApiService`.
///
/// Every implemented API method sends the command further via the `service_channel` to the
/// `ApiService` backend where it gets handled. The result is returned via the `back_channel` and
/// finally sent back to the client as a JSON RPC response.
impl Api for ApiService {
    fn get_entry_args(&self, params_raw: Params) -> BoxFuture<Result<EntryArgsResponse>> {
        let service_channel = self.service_channel.clone();

        Box::pin(async move {
            // Parse and validate incoming command parameters
            let params: EntryArgsRequest = params_raw.parse()?;
            params.validate()?;

            // Create back_channel to receive result from backend
            let (back_channel, back_channel_notifier) = unbounded();

            // Send request to backend and wait for response on back_channel
            task::block_on(
                service_channel.send(ApiServiceMessages::GetEntryArgs(params, back_channel)),
            )
            .unwrap();
            task::block_on(back_channel_notifier.recv()).unwrap()
        })
    }
}

/// Implementation of `p2panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the schemas
/// log_id) to encode a new bamboo entry.
async fn get_entry_args(pool: Pool, params: EntryArgsRequest) -> Result<EntryArgsResponse> {
    // Determine log_id for author's schema
    let log_id = Log::schema_log_id(&pool, &params.author, &params.schema).await?;

    // Find latest entry in this log
    let entry_latest = Entry::latest(&pool, &params.author, &log_id).await?;

    match entry_latest {
        Some(entry_backlink) => {
            // Determine skiplink ("lipmaa"-link) entry in this log
            let entry_skiplink = Entry::at_seq_num(
                &pool,
                &params.author,
                &log_id,
                &entry_backlink.seq_num.skiplink_seq_num(),
            )
            .await?
            .unwrap();

            Ok(EntryArgsResponse {
                entry_hash_backlink: Some(entry_backlink.entry_hash),
                entry_hash_skiplink: Some(entry_skiplink.entry_hash),
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
    use super::ApiService;

    use jsonrpc_core::ErrorCode;

    use crate::test_helpers::{initialize_db, random_entry_hash};

    const TEST_AUTHOR: &str = "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a";

    // Helper method to generate valid JSON RPC request string
    fn rpc_request(method: &str, params: &str) -> String {
        format!(
            r#"{{
                "jsonrpc": "2.0",
                "method": "{}",
                "params": {},
                "id": 1
            }}"#,
            method, params
        )
        .replace(" ", "")
        .replace("\n", "")
    }

    // Helper method to generate valid JSON RPC response string
    fn rpc_response(result: &str) -> String {
        format!(
            r#"{{
                "jsonrpc": "2.0",
                "result": {},
                "id": 1
            }}"#,
            result
        )
        .replace(" ", "")
        .replace("\n", "")
    }

    // Helper method to generate valid JSON RPC error response string
    fn rpc_error(code: ErrorCode, message: &str) -> String {
        format!(
            r#"{{
                "jsonrpc": "2.0",
                "error": {{
                    "code": {},
                    "message": "<message>"
                }},
                "id": 1
            }}"#,
            code.code(),
        )
        .replace(" ", "")
        .replace("\n", "")
        .replace("<message>", message)
    }

    #[async_std::test]
    async fn respond_with_missing_param_error() {
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

        let request = rpc_request(
            "panda_getEntryArguments",
            &format!(
                r#"{{
                    "schema": "{}"
                }}"#,
                random_entry_hash()
            ),
        );

        let response = rpc_error(
            ErrorCode::InvalidParams,
            "Invalid params: missing field `author`.",
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }

    #[async_std::test]
    async fn respond_with_wrong_author_error() {
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

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

        let response = rpc_error(
            ErrorCode::InvalidParams,
            "Invalid params: `author`: invalid string length, invalid ed25519 public key.",
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }

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
