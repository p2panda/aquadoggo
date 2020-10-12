use jsonrpc_core::futures::future::{self, FutureResult};
use jsonrpc_core::{IoHandler, Error, Result};
use jsonrpc_derive::rpc;

/// Node RPC API methods
#[rpc(server)]
pub trait NodeApi {
    #[rpc(name = "add")]
    fn add(&self, a: u64, b: u64) -> Result<u64>;

    #[rpc(name = "callAsync")]
    fn call(&self, a: u64) -> FutureResult<String, Error>;
}

/// Implementations of RPC methods
struct NodeApiService {
}

impl NodeApiService {
    pub fn new() -> Self {
        NodeApiService {}
    }
}

impl NodeApi for NodeApiService {
    fn add(&self, a: u64, b: u64) -> Result<u64> {
        Ok(a + b)
    }

    fn call(&self, _: u64) -> FutureResult<String, Error> {
        future::ok("OK".to_owned()).into()
    }
}

pub fn build_rpc_handler() -> IoHandler {
    let mut io = IoHandler::default();
    io.extend_with(NodeApi::to_delegate(NodeApiService::new()));
    io
}
