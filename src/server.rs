use jsonrpc_core::IoHandler;
use std::io::{ErrorKind, Result};
use std::net::SocketAddr;

/// Type alias for http server.
pub type HttpServer = http::Server;
/// Type alias for ws server.
pub type WebsocketServer = ws::Server;

/// Start HTTP RPC server listening on given address.
pub fn start_http(
    addr: &SocketAddr,
    io: IoHandler,
    threads: usize,
    max_payload: usize,
) -> Result<HttpServer> {
    http::ServerBuilder::new(io)
        .threads(threads)
        .max_request_body_size(max_payload)
        .start_http(addr)
}

/// Start WebSocket RPC server listening on given address.
pub fn start_ws(
    addr: &SocketAddr,
    max_connections: usize,
    max_payload: usize,
    io: IoHandler,
) -> Result<WebsocketServer> {
    ws::ServerBuilder::new(io)
        .max_payload(max_payload)
        .max_connections(max_connections)
        .start(addr)
        .map_err(|err| match err {
            ws::Error::Io(io) => io,
            ws::Error::ConnectionClosed => ErrorKind::BrokenPipe.into(),
            e => {
                error!("{}", e);
                ErrorKind::Other.into()
            }
        })
}
