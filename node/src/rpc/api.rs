use jsonrpc_core::futures::future::{self, FutureResult};
use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct EntryArgsParams {
    author: String,
    schema: String
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgs {
    encoded_entry_backlink: String,
    encoded_entry_skiplink: String,
    last_seq_num: u64,
    log_id: u64
}

/// Node RPC API methods.
#[rpc(server)]
pub trait Api {
    #[rpc(name = "add")]
    fn add(&self, a: u64, b: u64) -> Result<u64>;

    #[rpc(name = "callAsync")]
    fn call(&self, a: u64) -> FutureResult<String, Error>;

    #[rpc(name = "panda_nextEntryArguments")]
    fn next_entry_arguments(&self, params: EntryArgsParams) -> FutureResult<EntryArgs, Error>;
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
    fn add(&self, a: u64, b: u64) -> Result<u64> {
        Ok(a + b)
    }

    fn call(&self, _: u64) -> FutureResult<String, Error> {
        future::ok("OK".to_owned()).into()
    }

    fn next_entry_arguments(&self, _params: EntryArgsParams) -> FutureResult<EntryArgs, Error> {
        future::ok(EntryArgs { 
            encoded_entry_backlink: String::from("encoded_entry_backlink"),
            encoded_entry_skiplink: String::from("skiplink"),
            last_seq_num: 1,
            log_id: 0
        }).into()
    }
}
