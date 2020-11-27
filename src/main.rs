#[macro_use]
extern crate log;

use async_ctrlc::CtrlC;
use async_std::task;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;

mod rpc;
mod server;
mod tasks;

/// Default RPC API HTTP server port.
const DEFAULT_HTTP_PORT: u16 = 9123;

/// Default RPC API WebSocket server port.
const DEFAULT_WEBSOCKET_PORT: u16 = 9456;

#[derive(StructOpt, Debug)]
#[structopt(name = "p2panda Node", about = "Node server for the p2panda network")]
struct Opt {
    /// Port to bind RPC http server, 9123 by default.
    #[structopt(short, long)]
    http_port: Option<u16>,

    /// Port to bind RPC websocket server, 9456 by default.
    #[structopt(short, long)]
    ws_port: Option<u16>,
}

fn main() {
    env_logger::init();

    // Parse command line arguments
    let opt = Opt::from_args();
    let http_port = opt.http_port.unwrap_or(DEFAULT_HTTP_PORT);
    let ws_port = opt.ws_port.unwrap_or(DEFAULT_WEBSOCKET_PORT);

    let mut task_manager = tasks::TaskManager::new();

    // Start HTTP RPC Server
    let http_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), http_port);
    let http_server = server::start_http(&http_address, rpc::build_rpc_handler())
        .expect("Could not start HTTP server");
    let http_close_handle = http_server.close_handle();

    task_manager.spawn("HTTP RPC Server", async move {
        info!("Start HTTP server at {}", http_address);
        http_server.wait();
        Ok(())
    });

    // Start WebSocket RPC Server
    let ws_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), ws_port);
    let ws_server = server::start_ws(&ws_address, Some(128), rpc::build_rpc_handler())
        .expect("Could not start WebSocket server");
    let ws_close_handle = ws_server.close_handle();

    task_manager.spawn("WebSocket RPC Server", async move {
        info!("Start WebSocket server at {}", ws_address);
        ws_server.wait().unwrap();
        Ok(())
    });

    // Run this until [CTRL] + [C] got pressed
    task::block_on(async {
        let ctrlc = CtrlC::new().unwrap();
        ctrlc.await;
    });

    // Send close signals to RPC servers
    http_close_handle.close();
    ws_close_handle.close();

    // Wait until all tasks are gracefully shut down and exit
    task::block_on(task_manager.shutdown());
}
