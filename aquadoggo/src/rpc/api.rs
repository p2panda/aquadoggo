use std::convert::TryFrom;

use async_std::channel::{unbounded, Sender};
use async_std::task;
use jsonrpc_core::{BoxFuture, IoHandler, Params};
use jsonrpc_derive::rpc;
use p2panda_rs::atomic::{
    Author, Entry as EntryUnsigned, EntrySigned, Hash, LogId, Message, MessageEncoded, SeqNum,
    Validation,
};
use serde::{Deserialize, Serialize};

use crate::db::models::{Entry, Log};
use crate::db::Pool;
use crate::errors::Result;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum APIError {
    #[error("Could not find backlink entry in database")]
    BacklinkMissing,

    #[error("Could not find skiplink entry in database")]
    SkiplinkMissing,

    #[error("Claimed log_id for schema not the same as in database")]
    InvalidLogId,
}

/// Request body of `panda_getEntryArguments`.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    author: Author,
    schema: Hash,
}

/// Request body of `panda_publishEntry`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryRequest {
    entry_encoded: EntrySigned,
    message_encoded: MessageEncoded,
}

/// Response body of `panda_getEntryArguments`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    entry_hash_backlink: Option<Hash>,
    entry_hash_skiplink: Option<Hash>,
    last_seq_num: Option<SeqNum>,
    log_id: LogId,
}

/// Response body of `panda_publishEntry`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    // @TODO: Define response body
}

/// Trait defining all Node RPC API methods.
#[rpc(server)]
pub trait Api {
    #[rpc(name = "panda_getEntryArguments", params = "raw")]
    fn get_entry_args(&self, params: Params) -> BoxFuture<Result<EntryArgsResponse>>;

    #[rpc(name = "panda_publishEntry", params = "raw")]
    fn publish_entry(&self, params: Params) -> BoxFuture<Result<PublishEntryResponse>>;
}

/// Channel messages to send RPC command requests and their payloads from frontend `Api` to
/// `ApiService` backend.
///
/// Every message contains a `Sender` as a `back_channel` to send the response back to the RPC
/// frontend.
#[derive(Debug)]
enum ApiServiceMessages {
    GetEntryArgs(EntryArgsRequest, Sender<Result<EntryArgsResponse>>),
    PublishEntry(PublishEntryRequest, Sender<Result<PublishEntryResponse>>),
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
                    Ok(ApiServiceMessages::PublishEntry(params, back_channel)) => {
                        back_channel
                            .send(publish_entry(pool, params).await)
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
            params.author.validate()?;
            params.schema.validate()?;

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

    fn publish_entry(&self, params_raw: Params) -> BoxFuture<Result<PublishEntryResponse>> {
        let service_channel = self.service_channel.clone();

        Box::pin(async move {
            // Parse incoming command parameters
            let params: PublishEntryRequest = params_raw.parse()?;

            // Validate entry hex encoding
            params.entry_encoded.validate()?;

            // Validate message schema and hex encoding
            params.message_encoded.validate()?;

            // Create back_channel to receive result from backend
            let (back_channel, back_channel_notifier) = unbounded();

            // Send request to backend and wait for response on back_channel
            task::block_on(
                service_channel.send(ApiServiceMessages::PublishEntry(params, back_channel)),
            )
            .unwrap();
            task::block_on(back_channel_notifier.recv()).unwrap()
        })
    }
}

/// Implementation of `panda_getEntryArguments` RPC method.
///
/// Returns required data (backlink and skiplink entry hashes, last sequence number and the schemas
/// log_id) to encode a new bamboo entry.
async fn get_entry_args(pool: Pool, params: EntryArgsRequest) -> Result<EntryArgsResponse> {
    // Determine log_id for author's schema
    let log_id = Log::find_schema_log_id(&pool, &params.author, &params.schema).await?;

    // Find latest entry in this log
    let entry_latest = Entry::latest(&pool, &params.author, &log_id).await?;

    match entry_latest {
        Some(entry_backlink) => {
            // Determine skiplink ("lipmaa"-link) entry in this log
            let entry_skiplink = Entry::at_seq_num(
                &pool,
                &params.author,
                &log_id,
                // Unwrap as we know that an skiplink exists as soon as previous entry is given
                &entry_backlink.seq_num.skiplink_seq_num().unwrap(),
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

/// Implementation of `panda_publishEntry` RPC method.
async fn publish_entry(pool: Pool, params: PublishEntryRequest) -> Result<PublishEntryResponse> {
    println!("HUHu");
    // Handle error as this conversion validates message hash
    let entry = EntryUnsigned::try_from((&params.entry_encoded, Some(&params.message_encoded)))?;
    let message = Message::from(&params.message_encoded);

    println!("{:?} {:?}", entry, message);

    // Retreive author and schema
    let author = params.entry_encoded.author();
    let schema = message.schema();

    // Determine log_id for author's schema
    let schema_log_id = Log::get(&pool, &author, &schema).await?;

    // Check if log_id is the same as the previously claimed one (when given)
    if schema_log_id.is_some() && schema_log_id.as_ref() != Some(entry.log_id()) {
        Err(APIError::InvalidLogId)?;
    }

    println!("schema_log_id={:?}", schema_log_id);

    // Get related bamboo backlink and skiplink entries
    let entry_backlink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            &entry.log_id(),
            &entry.seq_num_backlink().unwrap(),
        )
        .await?
        .map(|link| {
            Some(
                hex::decode(link.entry_bytes)
                    .expect("Backlink entry with invalid hex-encoding detected in database"),
            )
        })
        .ok_or(APIError::BacklinkMissing)
    } else {
        Ok(None)
    }?;

    let entry_skiplink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            &entry.log_id(),
            &entry.seq_num_skiplink().unwrap(),
        )
        .await?
        .map(|link| {
            Some(
                hex::decode(link.entry_bytes)
                    .expect("Skiplink entry with invalid hex-encoding detected in database"),
            )
        })
        .ok_or(APIError::SkiplinkMissing)
    } else {
        Ok(None)
    }?;

    // Verify bamboo entry integrity
    bamboo_rs_core::verify(
        &params.entry_encoded.to_bytes(),
        Some(&params.message_encoded.to_bytes()),
        entry_skiplink_bytes.as_deref(),
        entry_backlink_bytes.as_deref(),
    )?;

    // Register used log id in database when not set yet
    if schema_log_id.is_none() {
        Log::insert(&pool, &author, &schema, entry.log_id()).await?;
    }

    // Finally insert Entry in database
    Entry::insert(
        &pool,
        &author,
        &params.entry_encoded,
        &params.entry_encoded.hash(),
        &entry.log_id(),
        &params.message_encoded,
        &params.message_encoded.hash(),
        &entry.seq_num(),
    )
    .await?;

    Ok(PublishEntryResponse {})
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use jsonrpc_core::ErrorCode;
    use p2panda_rs::atomic::{
        Entry, EntrySigned, Hash, LogId, Message, MessageEncoded, MessageFields, MessageValue,
        SeqNum,
    };
    use p2panda_rs::key_pair::KeyPair;

    use crate::test_helpers::{initialize_db, random_entry_hash};

    use super::ApiService;

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
            "Invalid params: invalid author key length.",
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

    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        skiplink: Option<&Hash>,
        backlink: Option<&Hash>,
        previous_seq_num: Option<&SeqNum>,
    ) -> (EntrySigned, MessageEncoded) {
        let mut fields = MessageFields::new();
        fields
            .add("test", MessageValue::Text("Hello".to_owned()))
            .unwrap();
        let message = Message::new_create(schema.clone(), fields).unwrap();
        let message_encoded = MessageEncoded::try_from(&message).unwrap();

        let first_entry =
            Entry::new(log_id, &message, skiplink, backlink, previous_seq_num).unwrap();
        let entry_encoded = EntrySigned::try_from((&first_entry, key_pair)).unwrap();

        println!(
            "{:?} {:?}",
            entry_encoded.as_str(),
            message_encoded.as_str()
        );

        (entry_encoded, message_encoded)
    }

    #[async_std::test]
    async fn publish_entry() {
        let key_pair = KeyPair::new();

        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::new(5);

        let (entry_encoded, message_encoded) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None);

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_response(r#"{}"#);

        assert_eq!(io.handle_request_sync(&request), Some(response));

        let (entry_encoded, message_encoded) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            None,
            Some(&entry_encoded.hash()),
            Some(&SeqNum::new(1).unwrap()),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "messageEncoded": "{}"
                }}"#,
                entry_encoded.as_str(),
                message_encoded.as_str(),
            ),
        );

        let response = rpc_response(r#"{}"#);

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }
}
