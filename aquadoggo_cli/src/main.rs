// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::uninlined_format_args)]
mod key_pair;
mod schemas;

use std::convert::TryFrom;
use std::net::IpAddr;

use anyhow::Result;
use aquadoggo::{Configuration, NetworkConfiguration, Node};
use clap::Parser;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};

const CONFIG_FILE_NAME: &str = "config.toml";
const CONFIG_ENV_VAR_PREFIX: &str = "DOGGO_";

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "aquadoggo Node", version)]
/// Node server for the p2panda network.
struct Cli {
    /// Port for the http server, 2020 by default.
    #[arg(short = 'P', long)]
    http_port: Option<u16>,

    /// Port for the QUIC transport, 2022 by default for a relay/rendezvous node.
    #[arg(short, long)]
    quic_port: Option<u16>,

    /// URLs of remote nodes to replicate with.
    #[arg(short, long)]
    remote_node_addresses: Vec<Multiaddr>,

    /// Enable mDNS for peer discovery over LAN (using port 5353), false by default.
    #[arg(short, long)]
    mdns: Option<bool>,

    /// Enable relay server to facilitate peer connectivity, false by default.
    #[arg(long)]
    enable_relay_server: bool,

    /// IP address for the relay peer.
    ///
    /// eg. --relay-address "127.0.0.1"
    #[arg(long)]
    relay_address: Option<IpAddr>,

    /// Port for the relay peer, defaults to expected relay port 2022.
    ///
    /// eg. --relay-port "1234"
    #[arg(long)]
    relay_port: Option<u16>,
}

impl TryFrom<Cli> for Configuration {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let mut config = Configuration::default();

        let relay_node = if let Some(relay_address) = cli.relay_address {
            let mut multiaddr = match relay_address {
                IpAddr::V4(ip) => Multiaddr::from(Protocol::Ip4(ip)),
                IpAddr::V6(ip) => Multiaddr::from(Protocol::Ip6(ip)),
            };
            multiaddr.push(Protocol::Udp(cli.relay_port.unwrap_or(2022)));
            multiaddr.push(Protocol::QuicV1);

            Some(multiaddr)
        } else {
            None
        };

        if let Some(http_port) = cli.http_port {
            config.http_port = http_port;
        }

        config.network = NetworkConfiguration {
            mdns: cli.mdns.unwrap_or(false),
            relay: cli.enable_relay_server,
            relay_node,
            trusted_nodes: cli.remote_node_addresses,
            ..config.network
        };

        if let Some(quic_port) = cli.quic_port {
            config.network.quic_port = quic_port;
        }

        Ok(config)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments
    let config: Result<Configuration, figment::Error> = Figment::new()
        .merge(Toml::file(CONFIG_FILE_NAME))
        .merge(Env::prefixed(CONFIG_ENV_VAR_PREFIX))
        .merge(Serialized::defaults(Cli::parse()))
        .extract();

    match config {
        Ok(config) => {
            let key_pair = key_pair::generate_ephemeral_key_pair();

            // Start p2panda node in async runtime
            let node = Node::start(key_pair, config).await;

            // Run this until [CTRL] + [C] got pressed or something went wrong
            tokio::select! {
                _ = tokio::signal::ctrl_c() => (),
                _ = node.on_exit() => (),
            }

            // Wait until all tasks are gracefully shut down and exit
            node.shutdown().await;
        }
        Err(error) => {
            println!("Error: Could not load configuration:");
            for error in error {
                println!("- {}", error);
            }
        }
    }
}
