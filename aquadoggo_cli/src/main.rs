// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::uninlined_format_args)]
use std::convert::{TryFrom, TryInto};

use anyhow::Result;
use aquadoggo::{Configuration, NetworkConfiguration, Node};
use clap::Parser;

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

    /// Enable mDNS for peer discovery over LAN (using port 5353), true by default.
    #[arg(short, long)]
    mdns: Option<bool>,

    /// Enable ping for connected peers (send and receive ping packets), true by default.
    #[arg(long)]
    ping: Option<bool>,

    /// Enable rendezvous client to facilitate peer discovery via a rendezvous server, false by default.
    #[arg(short = 'C', long)]
    rendezvous_client: Option<bool>,

    /// Enable rendezvous server to facilitate peer discovery for remote peers, false by default.
    #[arg(short = 'S', long)]
    rendezvous_server: Option<bool>,

    /// The IP address of a rendezvous server in the form of a multiaddress.
    ///
    /// eg. --rendezvous-address "/ip4/127.0.0.1/udp/12345/quic-v1"
    #[arg(long)]
    rendezvous_address: Option<String>,

    /// The peer ID of a rendezvous server in the form of an Ed25519 key encoded as a raw
    /// base58btc multihash.
    ///
    /// eg. --rendezvous-peer-id "12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA"
    #[arg(long)]
    rendezvous_peer_id: Option<String>,
}

impl TryFrom<Cli> for Configuration {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let mut config = Configuration::new(cli.data_dir)?;
        config.http_port = cli.http_port.unwrap_or(2020);

        config.network = NetworkConfiguration {
            mdns: cli.mdns.unwrap_or(true),
            ping: cli.ping.unwrap_or(true),
            quic_port: cli.quic_port.unwrap_or(2022),
            remote_peers: cli.remote_node_addresses,
            rendezvous_client: cli.rendezvous_client.unwrap_or(false),
            rendezvous_server: cli.rendezvous_server.unwrap_or(false),
            rendezvous_address: cli.rendezvous_address,
            rendezvous_peer_id: cli.rendezvous_peer_id,
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
