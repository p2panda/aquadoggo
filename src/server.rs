use jsonrpc_core::IoHandler;
use std::io;
use std::net::SocketAddr;

/// Maximal payload accepted by RPC servers.
const MAX_PAYLOAD: usize = 15 * 1024 * 1024;

/// Default maximum number of connections for WS RPC servers.
const WS_MAX_CONNECTIONS: usize = 100;

/// Type alias for http server
pub type HttpServer = http::Server;
/// Type alias for ws server
pub type WebsocketServer = ws::Server;

/// Start HTTP server listening on given address.
pub fn start_http(
    addr: &SocketAddr,
    cors: Option<&Vec<String>>,
    io: IoHandler,
) -> io::Result<HttpServer> {
    http::ServerBuilder::new(io)
        .threads(4)
        .allowed_hosts(hosts_filtering(cors.is_some()))
        .cors(map_cors::<http::AccessControlAllowOrigin>(cors))
        .max_request_body_size(MAX_PAYLOAD)
        .start_http(addr)
}

/// Start WS server listening on given address.
pub fn start_ws (
    addr: &SocketAddr,
    max_connections: Option<usize>,
    cors: Option<&Vec<String>>,
    io: IoHandler,
) -> io::Result<WebsocketServer> {
    ws::ServerBuilder::new(io)
        .max_payload(MAX_PAYLOAD)
        .max_connections(max_connections.unwrap_or(WS_MAX_CONNECTIONS))
        .allowed_origins(map_cors(cors))
        .allowed_hosts(hosts_filtering(cors.is_some()))
        .start(addr)
        .map_err(|err| match err {
            ws::Error::Io(io) => io,
            ws::Error::ConnectionClosed => io::ErrorKind::BrokenPipe.into(),
            e => {
                println!("{}", e);
                io::ErrorKind::Other.into()
            }
        })
}

fn map_cors<T: for<'a> From<&'a str>>(
    cors: Option<&Vec<String>>
) -> http::DomainsValidation<T> {
    cors.map(|x| x.iter().map(AsRef::as_ref).map(Into::into).collect::<Vec<_>>()).into()
}

fn hosts_filtering(enable: bool) -> http::DomainsValidation<http::Host> {
    if enable {
        // @NOTE: The listening address is whitelisted by default. Setting an empty vector here
        // enables the validation and allows only the listening address.
        http::DomainsValidation::AllowOnly(vec![])
    } else {
        http::DomainsValidation::Disabled
    }
}
