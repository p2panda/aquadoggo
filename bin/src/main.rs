use async_ctrlc::CtrlC;
use async_std::task;
use p2panda_core::Configuration;
use p2panda_node::Runtime;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "p2panda Node", about = "Node server for the p2panda network")]
struct Opt {
    /// Path to data folder, $HOME/.local/share/p2panda by default on Linux.
    #[structopt(short, long, parse(from_os_str))]
    data_dir: Option<std::path::PathBuf>,
}

#[async_std::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments and load configuration
    let opt = Opt::from_args();
    let config = Configuration::new(opt.data_dir).expect("Could not load configuration");

    // Start p2panda node in async runtime
    let node = Runtime::start(config).await;

    // Run this until [CTRL] + [C] got pressed
    task::block_on(async {
        let ctrlc = CtrlC::new().unwrap();
        ctrlc.await;
    });

    // Wait until all tasks are gracefully shut down and exit
    task::block_on(node.shutdown());
}
