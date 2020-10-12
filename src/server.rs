use jsonrpc_core::IoHandler;
use std::io::{Result, ErrorKind};
use std::net::SocketAddr;

/// Maximal payload accepted by RPC servers.
const MAX_PAYLOAD: usize = 15 * 1024 * 1024;

/// Default maximum number of connections for WS RPC servers.
const WS_MAX_CONNECTIONS: usize = 100;

/// Type alias for http server.
pub type HttpServer = http::Server;
/// Type alias for ws server.
pub type WebsocketServer = ws::Server;

/// Start HTTP server listening on given address.
pub fn start_http(addr: &SocketAddr, io: IoHandler) -> Result<HttpServer> {
    http::ServerBuilder::new(io)
        .threads(4)
        .max_request_body_size(MAX_PAYLOAD)
        .start_http(addr)
}

/// Start WS server listening on given address.
pub fn start_ws (
    addr: &SocketAddr,
    max_connections: Option<usize>,
    io: IoHandler,
) -> Result<WebsocketServer> {
    ws::ServerBuilder::new(io)
        .max_payload(MAX_PAYLOAD)
        .max_connections(max_connections.unwrap_or(WS_MAX_CONNECTIONS))
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
