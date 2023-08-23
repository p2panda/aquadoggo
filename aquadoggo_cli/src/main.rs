// SPDX-License-Identifier: AGPL-3.0-or-later

mod key_pair;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use anyhow::Result;
use aquadoggo::{AllowList, Configuration as NodeConfiguration, NetworkConfiguration, Node};
use clap::Parser;
use directories::ProjectDirs;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use p2panda_rs::schema::SchemaId;
use serde::{Deserialize, Serialize};

const CONFIG_FILE_NAME: &str = "config.toml";
const CONFIG_ENV_VAR_PREFIX: &str = "DOGGO_";

/// Node server for the p2panda network.
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "aquadoggo Node", version)]
struct Configuration {
    /// Path to config.toml file.
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,

    /// List of schema ids which a node will replicate and expose on the GraphQL API.
    #[arg(short = 's', long)]
    supported_schema_ids: Vec<SchemaId>,

    /// URL / connection string to PostgreSQL or SQLite database.
    #[arg(short = 'd', long, default_value = "sqlite::memory:")]
    database_url: String,

    /// Maximum number of connections that the database pool should maintain.
    #[arg(long, default_value_t = 32)]
    database_max_connections: u32,

    /// Number of concurrent workers which defines the maximum of materialization
    /// tasks which can be worked on simultaneously.
    #[arg(long, default_value_t = 16)]
    worker_pool_size: u32,

    /// HTTP port, serving the GraphQL API.
    #[arg(short = 'p', long, default_value_t = 2020)]
    http_port: u16,

    /// QUIC port for node-node communication and data replication.
    #[arg(short = 'q', long, default_value_t = 2022)]
    quic_port: u16,

    /// Path to persist your ed25519 private key file.
    #[arg(short = 'k', long)]
    private_key: Option<PathBuf>,

    /// mDNS to discover other peers on the local network.
    #[arg(short = 'm', long, default_value_t = true)]
    mdns: bool,

    /// List of addresses of trusted and known nodes.
    #[arg(short = 'n', long)]
    node_addresses: Vec<SocketAddr>,

    /// Address of relay.
    #[arg(short = 'r', long)]
    relay_address: Option<SocketAddr>,

    /// Set to true if our node should also function as a relay. Defaults to false.
    #[arg(short = 'e', long, default_value_t = false)]
    im_a_relay: bool,
}

impl From<Configuration> for NodeConfiguration {
    fn from(cli: Configuration) -> Self {
        let supported_schema_ids = if cli.supported_schema_ids.is_empty() {
            AllowList::Wildcard
        } else {
            AllowList::Set(cli.supported_schema_ids)
        };

        NodeConfiguration {
            database_url: cli.database_url,
            database_max_connections: cli.database_max_connections,
            http_port: cli.http_port,
            worker_pool_size: cli.worker_pool_size,
            supported_schema_ids,
            network: NetworkConfiguration {
                quic_port: cli.quic_port,
                mdns: cli.mdns,
                node_addresses: cli.relay_address.into_iter().map(to_multiaddress).collect(),
                im_a_relay: cli.im_a_relay,
                relay_address: cli.relay_address.map(to_multiaddress),
                ..Default::default()
            },
        }
    }
}

fn to_multiaddress(socket_address: SocketAddr) -> Multiaddr {
    let mut multiaddr = match socket_address.ip() {
        IpAddr::V4(ip) => Multiaddr::from(Protocol::Ip4(ip)),
        IpAddr::V6(ip) => Multiaddr::from(Protocol::Ip6(ip)),
    };
    multiaddr.push(Protocol::Udp(socket_address.port()));
    multiaddr.push(Protocol::QuicV1);
    multiaddr
}

fn try_determine_config_file_path() -> Option<PathBuf> {
    // Find config file in current folder
    let mut current_dir = std::env::current_dir().expect("Could not determine current directory");
    current_dir.push(CONFIG_FILE_NAME);

    // Find config file in XDG config folder
    let mut xdg_config_dir: PathBuf = ProjectDirs::from("", "", "aquadoggo")
        .expect("Could not determine valid config directory path from operating system")
        .config_dir()
        .to_path_buf();
    xdg_config_dir.push(CONFIG_FILE_NAME);

    [current_dir, xdg_config_dir]
        .iter()
        .find(|path| path.exists())
        .cloned()
}

fn load_config() -> Result<Configuration, figment::Error> {
    // Parse command line arguments first
    let cli = Configuration::parse();

    // Determine if a config file path was provided or if we should look for it in common locations
    let config_file_path = if cli.config.is_some() {
        cli.config.clone()
    } else {
        try_determine_config_file_path()
    };

    // Get configuration from .toml file (optional), environment variable and command line
    // arguments
    let mut figment = Figment::new();

    if let Some(path) = config_file_path {
        figment = figment.merge(Toml::file(path));
    }

    figment
        .merge(Env::prefixed(CONFIG_ENV_VAR_PREFIX))
        .merge(Serialized::defaults(cli))
        .extract()
}

fn panda_da() -> String {
    r#"
                           ██████ ███████ ████
                          ████████       ██████
                          ██████            ███
                           █████              ██
                           █     ████      █████
                          █     ██████   █ █████
                         ██      ████   ███ █████
                        █████         ██████    █
                       ███████                ██
                       █████████   █████████████
                       ███████████      █████████
                       █████████████████         ████
                  ██████    ███████████              ██
              ██████████        █████                 █
               █████████        ██          ███       ██
                 ██████        █            █           ██
                    ██       ██             ███████     ██
                  ███████████                      ██████
    ████████     ████████████                   ██████
    ████   ██████ ██████████            █   ████
      █████████   ████████       ███    ███████
        ████████             ██████    ████████
    █████████  ████████████████████████   ███
    █████████                      ██"#
        .into()
}

#[tokio::main]
async fn main() {
    env_logger::init();

    match load_config() {
        Ok(config) => {
            // @TODO: Nicer print
            println!("{}\n\n{:#?}", panda_da(), config);

            let key_pair = match &config.private_key {
                Some(path) => key_pair::generate_or_load_key_pair(path.clone())
                    .expect("Could not load private key from file"),
                None => key_pair::generate_ephemeral_key_pair(),
            };

            // Start p2panda node in async runtime
            let node = Node::start(key_pair, config.into()).await;

            // Run this until [CTRL] + [C] got pressed or something went wrong
            tokio::select! {
                _ = tokio::signal::ctrl_c() => (),
                _ = node.on_exit() => (),
            }

            // Wait until all tasks are gracefully shut down and exit
            node.shutdown().await;
        }
        Err(error) => {
            println!("Failed loading configuration:");

            for error in error {
                println!("- {}", error);
            }
        }
    }
}
