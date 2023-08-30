// SPDX-License-Identifier: AGPL-3.0-or-later

mod config;
mod key_pair;
mod utils;

use std::convert::TryInto;
use std::str::FromStr;

use anyhow::Context;
use aquadoggo::{AllowList, Configuration, Node};
use env_logger::WriteStyle;
use log::{warn, LevelFilter};

use crate::config::{load_config, print_config};
use crate::key_pair::{generate_ephemeral_key_pair, generate_or_load_key_pair};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration from command line arguments, environment variables and .toml file
    let (config_file_path, config) = load_config().context("Could not load configuration")?;

    // Set log verbosity based on config. By default scope it always to the "aquadoggo" module.
    let mut builder = env_logger::Builder::new();
    let builder = match LevelFilter::from_str(&config.log_level) {
        Ok(log_level) => builder.filter(Some("aquadoggo"), log_level),
        Err(_) => builder.parse_filters(&config.log_level),
    };
    builder.write_style(WriteStyle::Always).init();

    // Convert to `aquadoggo` configuration format and check for invalid inputs
    let node_config = config
        .clone()
        .try_into()
        .context("Could not load configuration")?;

    // Generate a new key pair, either just for this session or persisted. Folders are
    // automatically created when we picked a path
    let (key_pair_path, key_pair) = match &config.private_key {
        Some(path) => {
            let key_pair = generate_or_load_key_pair(path.clone())
                .context("Could not load private key from file")?;
            (Some(path), key_pair)
        }
        None => (None, generate_ephemeral_key_pair()),
    };

    // Show configuration info to the user
    println!(
        "{}",
        print_config(key_pair_path, config_file_path, &node_config)
    );
    show_warnings(&node_config);

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

/// Show some hopefully helpful warnings around common configuration issues.
fn show_warnings(config: &Configuration) {
    match &config.allow_schema_ids {
        AllowList::Set(values) => {
            if values.is_empty() && !config.network.relay_mode {
                warn!("Your node was set to not allow any schema ids which is only useful in combination with enabling relay mode. With this setting you will not be able to interact with any client or node.");
            }
        }
        AllowList::Wildcard => {
            warn!("Allowed schema ids is set to wildcard. Your node will support _any_ schemas it will encounter on the network. This is useful for experimentation and local development but _not_ recommended for production settings.");
        }
    }

    if !config.network.relay_addresses.is_empty() && config.network.relay_mode {
        warn!("Will not connect to given relay addresses when relay mode is enabled");
    }
}
