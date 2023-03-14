// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::uninlined_format_args)]
use std::convert::{TryFrom, TryInto};
use std::error::Error;

use anyhow::Result;
use aquadoggo::{Configuration, NetworkConfiguration, Node, ReplicationConfiguration};
use clap::Parser;

/// Helper method to parse a single key-value pair.
fn parse_key_val<T, U>(s: &str) -> Result<(T, Vec<U>), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let key = s[..pos].parse()?;
    let values = s[pos + 1..]
        .split(' ')
        .map(|elem| elem.parse())
        .collect::<Result<Vec<_>, _>>()?;

    Ok((key, values))
}

#[derive(Parser, Debug)]
#[command(name = "aquadoggo Node", version)]
/// Node server for the p2panda network.
struct Cli {
    /// Path to data folder, $HOME/.local/share/aquadoggo by default on Linux.
    #[arg(short, long)]
    data_dir: Option<std::path::PathBuf>,

    /// Port for the http server, 2020 by default.
    #[arg(short = 'P', long)]
    http_port: Option<u16>,

    /// Port for the QUIC transport, 2022 by default.
    #[arg(short, long)]
    quic_port: Option<u16>,

    /// URLs of remote nodes to replicate with.
    #[arg(short, long)]
    remote_node_addresses: Vec<String>,

    /// A collection of authors and their logs to replicate.
    ///
    /// eg. -A 123abc="1 2 345" -A 456def="6 7"
    /// .. adds the authors:
    /// - "123abc" with log_ids 1, 2, 345
    /// - "456def" with log_ids 6 7
    #[arg(short = 'A', value_parser = parse_key_val::<String, u64>, number_of_values = 1)]
    public_keys_to_replicate: Vec<(String, Vec<u64>)>,

    /// Enable mDNS for peer discovery over LAN (using port 5353), true by default.
    #[arg(short, long)]
    mdns: Option<bool>,

    /// Enable ping for connected peers (send and receive ping packets), true by default.
    #[arg(long)]
    ping: Option<bool>,
}

impl TryFrom<Cli> for Configuration {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let mut config = Configuration::new(cli.data_dir)?;
        config.http_port = cli.http_port.unwrap_or(2020);

        let public_keys_to_replicate = cli
            .public_keys_to_replicate
            .into_iter()
            .map(|elem| elem.try_into())
            .collect::<Result<_>>()?;

        config.replication = ReplicationConfiguration {
            remote_peers: cli.remote_node_addresses,
            public_keys_to_replicate,
            ..ReplicationConfiguration::default()
        };

        config.network = NetworkConfiguration {
            mdns: cli.mdns.unwrap_or(true),
            ping: cli.ping.unwrap_or(true),
            quic_port: cli.quic_port.unwrap_or(2022),
            ..NetworkConfiguration::default()
        };

        Ok(config)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments and load configuration
    let cli = Cli::parse();
    let config = cli.try_into().expect("Could not load configuration");

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
