#[macro_use] extern crate log;

use async_ctrlc::CtrlC;
use async_std::task;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;

mod rpc;
mod server;

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

#[async_std::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    // Parse command line arguments
    let opt = Opt::from_args();
    let http_port = opt.http_port.unwrap_or(DEFAULT_HTTP_PORT);
    let ws_port = opt.ws_port.unwrap_or(DEFAULT_WEBSOCKET_PORT);

    let exit_signal_handler = task::spawn(async {
        let ctrlc = CtrlC::new().unwrap();
        ctrlc.await;
        info!("Shut down node");
    });

    task::spawn(async move {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), http_port);
        server::start_http(&address, rpc::build_rpc_handler())
            .expect("Could not start http server")
            .wait();
    });

    task::spawn(async move {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), ws_port);
        server::start_ws(&address, Some(128), rpc::build_rpc_handler())
            .expect("Could not start websocket server")
            .wait()
            .unwrap();
    });

    exit_signal_handler.await;

    Ok(())
}
