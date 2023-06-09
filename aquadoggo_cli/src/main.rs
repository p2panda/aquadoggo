// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::uninlined_format_args)]
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;

use anyhow::Result;
use aquadoggo::{Configuration, NetworkConfiguration, Node};
use clap::error::ErrorKind as ClapErrorKind;
use clap::{CommandFactory, Parser};
use libp2p::{Multiaddr, PeerId};

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
    remote_node_addresses: Vec<Multiaddr>,

    /// Enable AutoNAT to facilitate NAT status determination, true by default.
    #[arg(short, long)]
    autonat: Option<bool>,

    /// Enable mDNS for peer discovery over LAN (using port 5353), true by default.
    #[arg(short, long)]
    mdns: Option<bool>,

    /// Enable ping for connected peers (send and receive ping packets), false by default.
    #[arg(long)]
    ping: Option<bool>,

    /// Enable rendezvous server to facilitate peer discovery for remote peers, false by default.
    #[arg(long)]
    enable_rendezvous_server: bool,

    /// The IP address and peer ID of a rendezvous server in the form of a multiaddress.
    ///
    /// eg. --rendezvous-address "/ip4/127.0.0.1/udp/12345/quic-v1/p2p/12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA"
    #[arg(long)]
    rendezvous_address: Option<Multiaddr>,

    /// Enable relay server to facilitate peer connectivity, false by default.
    #[arg(long)]
    enable_relay_server: bool,

    /// The IP address and peer ID of a relay server in the form of a multiaddress.
    ///
    /// eg. --relay-address "/ip4/127.0.0.1/udp/12345/quic-v1/p2p/12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA"
    #[arg(long)]
    relay_address: Option<Multiaddr>,
}

impl Cli {
    // Run custom validators on parsed CLI input
    fn validate(self) -> Self {
        // @TODO: This needs updating for v0.5.2 https://github.com/libp2p/rust-libp2p/pull/3656
        //         // Ensure rendezvous server address includes a peer ID
        //         if let Some(addr) = &self.rendezvous_address {
        //             // Check if the given `Multiaddr` contains a `PeerId`
        //             if PeerId::try_from_multiaddr(addr).is_none() {
        //                 // Print a help message about the missing value(s) and exit
        //                 Cli::command()
        //                     .error(
        //                         ClapErrorKind::ValueValidation,
        //                         "'--rendezvous-address' must include the peer ID of the server",
        //                     )
        //                     .exit()
        //             }
        //         }
        //
        //         // Ensure relay server address includes a peer ID
        //         if let Some(addr) = &self.relay_address {
        //             // Check if the given `Multiaddr` contains a `PeerId`
        //             if PeerId::try_from_multiaddr(addr).is_none() {
        //                 // Print a help message about the missing value(s) and exit
        //                 Cli::command()
        //                     .error(
        //                         ClapErrorKind::ValueValidation,
        //                         "'--relay-address' must include the peer ID of the server",
        //                     )
        //                     .exit()
        //             }
        //         }

        self
    }
}

impl TryFrom<Cli> for Configuration {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let mut config = Configuration::new(cli.data_dir)?;

        let relay_peer_id = if let Some(addr) = &cli.relay_address {
            // @TODO: This needs updating for v0.5.2 https://github.com/libp2p/rust-libp2p/pull/3656

            Some(PeerId::from_str(&addr.into_iter().last().unwrap().to_string()).unwrap())
        } else {
            None
        };

        let rendezvous_peer_id = if let Some(addr) = &cli.rendezvous_address {
            // @TODO: This needs updating for v0.5.2 https://github.com/libp2p/rust-libp2p/pull/3656
            Some(PeerId::from_str(&addr.into_iter().last().unwrap().to_string()).unwrap())
        } else {
            None
        };

        config.http_port = cli.http_port.unwrap_or(2020);

        config.network = NetworkConfiguration {
            autonat: cli.autonat.unwrap_or(true),
            mdns: cli.mdns.unwrap_or(true),
            ping: cli.ping.unwrap_or(false),
            quic_port: cli.quic_port.unwrap_or(2022),
            relay_address: cli.relay_address,
            relay_peer_id,
            relay_server_enabled: cli.enable_relay_server,
            remote_peers: cli.remote_node_addresses,
            rendezvous_address: cli.rendezvous_address,
            rendezvous_peer_id,
            rendezvous_server_enabled: cli.enable_rendezvous_server,
            ..config.network
        };

        Ok(config)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments and run custom validators
    let cli = Cli::parse().validate();

    // Load configuration parameters and apply defaults
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
