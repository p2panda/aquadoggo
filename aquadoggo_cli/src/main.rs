// SPDX-License-Identifier: AGPL-3.0-or-later

use structopt::StructOpt;

use aquadoggo::{Configuration, Node};

#[derive(StructOpt, Debug)]
#[structopt(name = "aquadoggo Node", about = "Node server for the p2panda network")]
struct Opt {
    /// Path to data folder, $HOME/.local/share/aquadoggo by default on Linux.
    #[structopt(short, long, parse(from_os_str))]
    data_dir: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments and load configuration
    let opt = Opt::from_args();
    let config = Configuration::new(opt.data_dir).expect("Could not load configuration");

    // Start p2panda node in async runtime
    let node = Node::start(config).await;

    // Run this until [CTRL] + [C] got pressed or something went wrong
    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
        _ = node.on_exit() => (),
    }

    // Wait until all tasks are gracefully shut down and exit
    node.shutdown().await;
}
