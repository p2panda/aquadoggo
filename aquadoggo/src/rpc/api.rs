use async_std::channel::{unbounded, Sender};
use async_std::task;
use jsonrpc_core::{BoxFuture, IoHandler, Params};
use jsonrpc_derive::rpc;
use p2panda_rs::atomic::Validation;

use crate::db::Pool;
use crate::errors::Result;
use crate::rpc::methods::{get_entry_args, publish_entry};
use crate::rpc::request::{EntryArgsRequest, PublishEntryRequest};
use crate::rpc::response::{EntryArgsResponse, PublishEntryResponse};

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

#[cfg(test)]
mod tests {
    use jsonrpc_core::ErrorCode;

    use crate::test_helpers::{initialize_db, random_entry_hash, rpc_error, rpc_request};

    use super::ApiService;

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
}
