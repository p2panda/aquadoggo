// SPDX-License-Identifier: AGPL-3.0-or-later

mod config;
mod key_pair;
mod utils;

use std::convert::TryInto;

use anyhow::Context;
use aquadoggo::{AllowList, Node};
use log::warn;

use crate::config::{load_config, print_config};
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

    // Generate a new key pair, either just for this session or persisted. Folders are
    // automatically created when we picked a path
    let key_pair = match &config.private_key {
        Some(path) => generate_or_load_key_pair(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_key_pair(),
    };

    // Show configuration info to the user
    println!("{}", print_config(config_file_path, &node_config));

    // Show some hopefully helpful warnings
    match &node_config.allow_schema_ids {
        AllowList::Set(values) => {
            if values.is_empty() && !node_config.network.relay_mode {
                warn!("Your node was set to not allow any schema ids which is only useful in combination with enabling relay mode. With this setting you will not be able to interact with any client or node.");
            }
        }
        AllowList::Wildcard => {
            warn!("Allowed schema ids is set to wildcard. Your node will support _any_ schemas it will encounter on the network. This is useful for experimentation and local development but _not_ recommended for production settings.");
        }
    }

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
