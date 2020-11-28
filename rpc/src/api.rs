use jsonrpc_core::futures::future::{self, FutureResult};
use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;

/// Node RPC API methods
#[rpc(server)]
pub trait Api {
    #[rpc(name = "add")]
    fn add(&self, a: u64, b: u64) -> Result<u64>;

    #[rpc(name = "callAsync")]
    fn call(&self, a: u64) -> FutureResult<String, Error>;
}

/// Implementations of RPC methods
pub struct ApiService {}

impl ApiService {
    pub fn new() -> Self {
        ApiService {}
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

pub fn build_rpc_handler() -> IoHandler {
    let mut io = IoHandler::default();
    io.extend_with(Api::to_delegate(ApiService::new()));
    io
}
