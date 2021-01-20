use jsonrpc_core::IoHandler;
use log::error;
use std::io::{ErrorKind, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::config::Configuration;
use crate::task::TaskManager;

/// Type alias for http server close handle.
type HttpCloseHandle = jsonrpc_http_server::CloseHandle;
/// Type alias for ws server close handle.
type WebSocketCloseHandle = jsonrpc_ws_server::CloseHandle;

/// Wrapper around jsonrpc-http-server implementation.
struct HttpServer {
    inner: jsonrpc_http_server::Server,
}

impl HttpServer {
    /// Start HTTP RPC server listening on given address.
    pub fn start(config: &Configuration, io: IoHandler) -> Result<Self> {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);

        let inner = jsonrpc_http_server::ServerBuilder::new(io)
            .threads(config.http_threads)
            .max_request_body_size(config.rpc_max_payload)
            .start_http(&address)?;

        Ok(Self { inner })
    }

    /// Keep server alive by running this blocking method.
    pub fn wait(self) {
        self.inner.wait();
    }

    /// Returns separate handler which can be used to close the server.
    pub fn close_handle(&self) -> HttpCloseHandle {
        self.inner.close_handle()
    }
}

/// Wrapper around jsonrpc-ws-server implementation.
struct WebSocketServer {
    inner: jsonrpc_ws_server::Server,
}

impl WebSocketServer {
    /// Start WebSocket RPC server listening on given address.
    pub fn start(config: &Configuration, io: IoHandler) -> Result<Self> {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.ws_port);

        let inner = jsonrpc_ws_server::ServerBuilder::new(io)
            .max_payload(config.rpc_max_payload)
            .max_connections(config.ws_max_connections)
            .start(&address)
            .map_err(|err| match err {
                jsonrpc_ws_server::Error::Io(io) => io,
                jsonrpc_ws_server::Error::ConnectionClosed => ErrorKind::BrokenPipe.into(),
                err => {
                    error!("{}", err);
                    ErrorKind::Other.into()
                }
            })?;

        Ok(Self { inner })
    }

    /// Keep server alive by running this blocking method.
    pub fn wait(self) {
        self.inner.wait().unwrap();
    }

    /// Returns separate handler which can be used to close the server.
    pub fn close_handle(&self) -> WebSocketCloseHandle {
        self.inner.close_handle()
    }
}

/// Manages both HTTP and WebSocket server to expose the JSON RPC API.
pub struct RpcServer {
    close_handle_http: HttpCloseHandle,
    close_handle_ws: WebSocketCloseHandle,
}

impl RpcServer {
    /// Spawn two separate tasks running HTTP and WebSocket servers both exposing a JSON RPC API.
    pub fn start(config: &Configuration, task_manager: &mut TaskManager, io: IoHandler) -> Self {
        // Start HTTP RPC server
        let http_server = HttpServer::start(&config, io.clone())
            .expect("Could not start HTTP JSON RPC API server");
        let close_handle_http = http_server.close_handle();

        task_manager.spawn("HTTP RPC Server", async move {
            http_server.wait();
            Ok(())
        });

        // Start WebSocket RPC server
        let ws_server = WebSocketServer::start(&config, io)
            .expect("Could not start WebSocket JSON RPC API server");
        let close_handle_ws = ws_server.close_handle();

        task_manager.spawn("WebSocket RPC Server", async move {
            ws_server.wait();
            Ok(())
        });

        // Keep close handlers for later
        Self {
            close_handle_http,
            close_handle_ws,
        }
    }

    /// Send close signals to all RPC servers.
    pub fn shutdown(self) {
        self.close_handle_http.close();
        self.close_handle_ws.close();
    }
}
