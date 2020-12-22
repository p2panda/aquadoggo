use jsonrpc_core::futures::future;
use jsonrpc_core::{BoxFuture, IoHandler, Params, Result};
use jsonrpc_derive::rpc;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct EntryArgsParams {
    author: String,
    schema: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgs {
    encoded_entry_backlink: String,
    encoded_entry_skiplink: String,
    last_seq_num: u64,
    log_id: u64,
}

/// Node RPC API methods.
#[rpc(server)]
pub trait Api {
    #[rpc(name = "panda_nextEntryArguments", params = "raw")]
    fn next_entry_arguments(&self, params: Params) -> BoxFuture<Result<EntryArgs>>;
}

/// Service implementing API methods.
pub struct ApiService {}

impl ApiService {
    /// Creates a JSON RPC API service.
    pub fn new() -> Self {
        ApiService {}
    }

    /// Creates JSON RPC API service and wraps it around a jsonrpc_core IoHandler object which can
    /// be used for a server exposing the API.
    pub fn io_handler() -> IoHandler {
        let mut io = IoHandler::default();
        io.extend_with(Api::to_delegate(ApiService::new()));
        io
    }
}

impl Api for ApiService {
    fn next_entry_arguments(&self, params: Params) -> BoxFuture<Result<EntryArgs>> {
        let _parsed: EntryArgsParams = params.parse().unwrap();

        Box::pin(future::ready(
            Ok(EntryArgs {
                encoded_entry_backlink: String::from("encoded_entry_backlink"),
                encoded_entry_skiplink: String::from("skiplink"),
                last_seq_num: 1,
                log_id: 0,
            })
            .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::ApiService;

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
    fn rpc_response<'a>(result: &str) -> String {
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

    #[test]
    fn next_entry_arguments() {
        let io = ApiService::io_handler();

        let request = rpc_request(
            "panda_nextEntryArguments",
            r#"
            {
                "author": "world",
                "schema": "test"
            }
            "#,
        );

        let response = rpc_response(
            r#"
            {
                "encodedEntryBacklink": "encoded_entry_backlink",
                "encodedEntrySkiplink": "skiplink",
                "lastSeqNum": 1,
                "logId": 0
            }
            "#,
        );

        assert_eq!(io.handle_request_sync(&request), Some(response));
    }
}
