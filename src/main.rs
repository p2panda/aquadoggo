#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use async_ctrlc::CtrlC;
use async_std::task;
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
    let rpc_server = server::Server::new(configuration, &mut task_manager);

    // Run this until [CTRL] + [C] got pressed
    task::block_on(async {
        let ctrlc = CtrlC::new().unwrap();
        ctrlc.await;
    });

    rpc_server.shutdown();

    // Wait until all tasks are gracefully shut down and exit
    task::block_on(task_manager.shutdown());

    Ok(())
}
