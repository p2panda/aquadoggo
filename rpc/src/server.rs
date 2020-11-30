use jsonrpc_core::IoHandler;
use std::io::{ErrorKind, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use p2panda_core::{Configuration, TaskManager};

/// Type alias for http server close handle.
type HttpCloseHandle = http::CloseHandle;
/// Type alias for ws server close handle.
type WebSocketCloseHandle = ws::CloseHandle;

struct HttpServer {
    inner: http::Server,
}

impl HttpServer {
    /// Start HTTP RPC server listening on given address.
    pub fn new(config: &Configuration, io: IoHandler) -> Result<Self> {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.http_port);

        let inner = http::ServerBuilder::new(io)
            .threads(config.http_threads)
            .max_request_body_size(config.rpc_max_payload)
            .start_http(&address)?;

        Ok(Self { inner })
    }

    pub fn wait(self) {
        self.inner.wait();
    }

    pub fn close_handle(&self) -> HttpCloseHandle {
        self.inner.close_handle()
    }
}

struct WebSocketServer {
    inner: ws::Server,
}

impl WebSocketServer {
    /// Start WebSocket RPC server listening on given address.
    pub fn new(config: &Configuration, io: IoHandler) -> Result<Self> {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.ws_port);

        let inner = ws::ServerBuilder::new(io)
            .max_payload(config.rpc_max_payload)
            .max_connections(config.ws_max_connections)
            .start(&address)
            .map_err(|err| match err {
                ws::Error::Io(io) => io,
                ws::Error::ConnectionClosed => ErrorKind::BrokenPipe.into(),
                err => {
                    error!("{}", err);
                    ErrorKind::Other.into()
                }
            })?;

        Ok(Self { inner })
    }

    pub fn wait(self) {
        self.inner.wait().unwrap();
    }

    pub fn close_handle(&self) -> WebSocketCloseHandle {
        self.inner.close_handle()
    }
}

pub struct RpcServer {
    close_handle_http: HttpCloseHandle,
    close_handle_ws: WebSocketCloseHandle,
}

impl RpcServer {
    pub fn start(config: &Configuration, task_manager: &mut TaskManager, io: IoHandler) -> Self {
        // Start HTTP RPC server
        let http_server =
            HttpServer::new(&config, io.clone()).expect("Could not start HTTP RPC server");
        let close_handle_http = http_server.close_handle();

        task_manager.spawn("HTTP RPC Server", async move {
            http_server.wait();
            Ok(())
        });

        // Start WebSocket RPC server
        let ws_server =
            WebSocketServer::new(&config, io.clone()).expect("Could not start WebSocket server");
        let close_handle_ws = ws_server.close_handle();

        task_manager.spawn("WebSocket RPC Server", async move {
            ws_server.wait();
            Ok(())
        });

        Self {
            close_handle_http,
            close_handle_ws,
        }
    }

    /// Send close signals to all RPC servers
    pub fn shutdown(self) {
        self.close_handle_http.close();
        self.close_handle_ws.close();
    }
}
