use jsonrpc_core::futures::future::{self, FutureResult};
use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;

/// Node RPC API methods.
#[rpc(server)]
pub trait Api {
    #[rpc(name = "add")]
    fn add(&self, a: u64, b: u64) -> Result<u64>;

    #[rpc(name = "callAsync")]
    fn call(&self, a: u64) -> FutureResult<String, Error>;
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
}
