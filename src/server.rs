use jsonrpc_core::IoHandler;
use std::io::{ErrorKind, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Type alias for http server.
type HttpServer = http::Server;
/// Type alias for ws server.
type WebsocketServer = ws::Server;

/// Start HTTP RPC server listening on given address.
fn start_http(
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
fn start_ws(
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
            err => {
                error!("{}", err);
                ErrorKind::Other.into()
            }
        })
}

pub struct Server {
    close_handle_http: http::CloseHandle,
    close_handle_ws: ws::CloseHandle,
}

impl Server {
    pub fn new(
        configuration: super::config::Configuration,
        task_manager: &mut super::tasks::TaskManager,
    ) -> Self {
        // Start HTTP RPC Server
        let http_address = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            configuration.server.http_port,
        );
        let http_server = start_http(
            &http_address,
            super::rpc::build_rpc_handler(),
            configuration.server.http_threads,
            configuration.server.max_payload,
        )
        .expect("Could not start HTTP server");
        let close_handle_http = http_server.close_handle();

        task_manager.spawn("HTTP RPC Server", async move {
            info!("Start HTTP server at {}", http_address);
            http_server.wait();
            Ok(())
        });

        // Start WebSocket RPC Server
        let ws_address = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            configuration.server.ws_port,
        );
        let ws_server = start_ws(
            &ws_address,
            configuration.server.ws_max_connections,
            configuration.server.max_payload,
            super::rpc::build_rpc_handler(),
        )
        .expect("Could not start WebSocket server");
        let close_handle_ws = ws_server.close_handle();

        task_manager.spawn("WebSocket RPC Server", async move {
            info!("Start WebSocket server at {}", ws_address);
            ws_server.wait().unwrap();
            Ok(())
        });

        Self {
            close_handle_http,
            close_handle_ws,
        }
    }

    pub fn shutdown(self) {
        // Send close signals to RPC servers
        self.close_handle_http.close();
        self.close_handle_ws.close();
    }
}
