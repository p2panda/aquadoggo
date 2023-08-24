// SPDX-License-Identifier: AGPL-3.0-or-later

mod config;
mod key_pair;

use std::convert::TryInto;

use anyhow::Context;
use aquadoggo::Node;
use clap::crate_version;

use crate::config::load_config;
use crate::key_pair::{generate_ephemeral_key_pair, generate_or_load_key_pair};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Load configuration from command line arguments, environment variables and .toml file
    let (config_file_path, config) = load_config().context("Could not load configuration")?;

    // Convert to `aquadoggo` configuration format and check for invalid inputs
    let node_config = config
        .clone()
        .try_into()
        .context("Could not load configuration")?;

    // @TODO: Create folders when paths for db or key was set
    let key_pair = match &config.private_key {
        Some(path) => generate_or_load_key_pair(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_key_pair(),
    };

    // Show configuration info to the user
    println!("aquadoggo v{}\n", crate_version!());
    match config_file_path {
        Some(path) => {
            println!("Loading config file from {}", path.display());
        }
        None => {
            println!("No config file provided");
        }
    }
    // @TODO: Improve print
    println!("{:?}", config);

    // Start p2panda node in async runtime
    let node = Node::start(key_pair, node_config).await;

    // Run this until [CTRL] + [C] got pressed or something went wrong
    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
        _ = node.on_exit() => (),
    }

    // Wait until all tasks are gracefully shut down and exit
    node.shutdown().await;

    Ok(())
}
