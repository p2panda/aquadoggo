#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use async_ctrlc::CtrlC;
use async_std::task;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;

mod config;
mod rpc;
mod server;
mod tasks;

#[derive(StructOpt, Debug)]
#[structopt(name = "p2panda Node", about = "Node server for the p2panda network")]
struct Opt {
    /// Path to data folder, $HOME/.local/share/p2panda by default on Linux.
    #[structopt(short, long, parse(from_os_str))]
    data_dir: Option<std::path::PathBuf>,
}

#[async_std::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    // Parse command line arguments
    let opt = Opt::from_args();
    let configuration =
        config::Configuration::new(opt.data_dir).expect("Could not load configuration");

    let mut task_manager = tasks::TaskManager::new();

    // Start HTTP RPC Server
    let http_address = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        configuration.server.http_port,
    );
    let http_server = server::start_http(
        &http_address,
        rpc::build_rpc_handler(),
        configuration.server.http_threads,
        configuration.server.max_payload,
    )
    .expect("Could not start HTTP server");
    let http_close_handle = http_server.close_handle();

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
    let ws_server = server::start_ws(
        &ws_address,
        configuration.server.ws_max_connections,
        configuration.server.max_payload,
        rpc::build_rpc_handler(),
    )
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

    Ok(())
}
