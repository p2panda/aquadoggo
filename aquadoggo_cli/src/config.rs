// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use aquadoggo::{AllowList, Configuration as NodeConfiguration, NetworkConfiguration};
use clap::{crate_version, Parser};
use colored::Colorize;
use directories::ProjectDirs;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use p2panda_rs::schema::SchemaId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::utils::absolute_path;

const WILDCARD: &str = "*";

const CONFIG_FILE_NAME: &str = "config.toml";

type ConfigFilePath = Option<PathBuf>;

/// Get configuration from 1. .toml file, 2. environment variables and 3. command line arguments
/// (in that order, meaning that later configuration sources take precedence over the earlier
/// ones).
///
/// Returns a partly unchecked configuration object which results from all of these sources. It
/// still needs to be converted for aquadoggo as it might still contain invalid values.
pub fn load_config() -> Result<(ConfigFilePath, Configuration)> {
    // Parse command line arguments first to get optional config file path
    let cli = Cli::parse();

    // Determine if a config file path was provided or if we should look for it in common locations
    let config_file_path: ConfigFilePath = match &cli.config {
        Some(path) => {
            if !path.exists() {
                bail!("Config file '{}' does not exist", path.display());
            }

            Some(path.clone())
        }
        None => try_determine_config_file_path(),
    };

    let mut figment = Figment::from(Serialized::defaults(Configuration::default()));
    if let Some(path) = &config_file_path {
        figment = figment.merge(Toml::file(path));
    }

    let config = figment
        .merge(Env::raw())
        .merge(Serialized::defaults(cli))
        .extract()?;

    Ok((config_file_path, config))
}

/// Configuration derived from command line arguments.
///
/// All arguments are optional and don't get serialized to Figment when they're None. This is to
/// assure that default values do not overwrite all previous settings, especially when they haven't
/// been set.
#[derive(Parser, Serialize, Debug)]
#[command(
    name = "aquadoggo Node",
    about = "Node server for the p2panda network",
    version
)]
struct Cli {
    /// Path to a config.toml file.
    #[arg(short = 'c', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<PathBuf>,

    /// List of schema ids which a node will replicate and expose on the GraphQL API.
    #[arg(short = 's', long, value_name = "SCHEMA_ID SCHEMA_ID ...", num_args = 0..)]
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_with_wildcard"
    )]
    allow_schema_ids: Option<Vec<String>>,

    /// URL / connection string to PostgreSQL or SQLite database.
    #[arg(short = 'd', long, value_name = "CONNECTION_STRING")]
    #[serde(skip_serializing_if = "Option::is_none")]
    database_url: Option<String>,

    /// HTTP port for client-node communication, serving the GraphQL API.
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    http_port: Option<u16>,

    /// QUIC port for node-node communication and data replication.
    #[arg(short = 'q', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    quic_port: Option<u16>,

    /// Path to persist your ed25519 private key file.
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    /// mDNS to discover other peers on the local network.
    #[arg(
        short = 'm',
        long,
        value_name = "BOOL",
        default_missing_value = "true",
        num_args = 0..=1,
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    mdns: Option<bool>,

    /// List of known node addresses we want to connect to directly.
    #[arg(short = 'n', long, value_name = "IP:PORT IP:PORT ...", num_args = 0..)]
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_node_addresses: Option<Vec<SocketAddr>>,

    /// Address of relay.
    #[arg(short = 'r', long, value_name = "IP:PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_address: Option<SocketAddr>,

    /// Set to true if our node should also function as a relay.
    #[arg(
        short = 'e',
        long,
        value_name = "BOOL",
        default_missing_value = "true",
        num_args = 0..=1,
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_mode: Option<bool>,
}

/// Clap converts wildcard symbols from command line arguments (for example --supported-schema-ids
/// "*") into an array, (["*"]), but we need it to be just a string ("*").
fn serialize_with_wildcard<S>(
    list: &Option<Vec<String>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match list {
        Some(list) => {
            // Wildcard symbol comes in form of an array ["*"], convert it to just a string "*"
            if list.len() == 1 && list[0] == WILDCARD {
                serializer.serialize_str(WILDCARD)
            } else {
                list.serialize(serializer)
            }
        }
        None => unreachable!("Serialization is skipped if value is None"),
    }
}

/// Configuration derived from environment variables and .toml file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    pub allow_schema_ids: UncheckedAllowList,
    pub database_url: String,
    pub database_max_connections: u32,
    pub worker_pool_size: u32,
    pub http_port: u16,
    pub quic_port: u16,
    pub private_key: Option<PathBuf>,
    pub mdns: bool,
    pub direct_node_addresses: Vec<SocketAddr>,
    pub relay_address: Option<SocketAddr>,
    pub relay_mode: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            allow_schema_ids: UncheckedAllowList::Wildcard,
            database_url: "sqlite::memory:".into(),
            database_max_connections: 32,
            worker_pool_size: 16,
            http_port: 2020,
            quic_port: 2022,
            private_key: None,
            mdns: true,
            direct_node_addresses: vec![],
            relay_address: None,
            relay_mode: false,
        }
    }
}

impl TryFrom<Configuration> for NodeConfiguration {
    type Error = anyhow::Error;

    fn try_from(value: Configuration) -> Result<Self, Self::Error> {
        // Check if given schema ids are valid
        let allow_schema_ids = match value.allow_schema_ids {
            UncheckedAllowList::Wildcard => AllowList::<SchemaId>::Wildcard,
            UncheckedAllowList::Set(str_values) => {
                let schema_ids: Result<Vec<SchemaId>, anyhow::Error> = str_values
                    .iter()
                    .map(|str_value| {
                        SchemaId::from_str(str_value).map_err(|_| {
                            anyhow!(
                                "Invalid schema id '{str_value}' found in 'allow_schema_ids' list"
                            )
                        })
                    })
                    .collect();

                AllowList::Set(schema_ids?)
            }
        };

        Ok(NodeConfiguration {
            database_url: value.database_url,
            database_max_connections: value.database_max_connections,
            http_port: value.http_port,
            worker_pool_size: value.worker_pool_size,
            allow_schema_ids,
            network: NetworkConfiguration {
                quic_port: value.quic_port,
                mdns: value.mdns,
                direct_node_addresses: value
                    .direct_node_addresses
                    .into_iter()
                    .map(to_multiaddress)
                    .collect(),
                relay_mode: value.relay_mode,
                relay_address: value.relay_address.map(to_multiaddress),
                ..Default::default()
            },
        })
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

pub fn print_config(path: ConfigFilePath, config: &NodeConfiguration) -> String {
    println!(
        r"                       ██████ ███████ ████
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
█████████                      ██
    "
    );

    println!("{} v{}\n", "aquadoggo".underline(), crate_version!());

    match path {
        Some(path) => {
            println!(
                "Loading config file from {}",
                absolute_path(path).display().to_string().blue()
            );
        }
        None => {
            println!("No config file provided");
        }
    }

    println!();
    println!("{}\n", "Configuration".underline());

    let allow_schema_ids: String = match &config.allow_schema_ids {
        AllowList::Set(schema_ids) => {
            if schema_ids.is_empty() {
                "none (disable replication)".into()
            } else {
                String::from("\n")
                    + &schema_ids
                        .iter()
                        .map(|id| format!("• {id}"))
                        .collect::<Vec<String>>()
                        .join("\n")
            }
        }
        AllowList::Wildcard => "* (any schema id)".into(),
    };

    let database_url = if config.database_url == "sqlite::memory:" {
        "memory (data is not persisted)".into()
    } else if config.database_url.contains("sqlite:") {
        format!("SQLite: {}", config.database_url)
    } else {
        "PostgreSQL".into()
    };

    let mdns = if config.network.mdns {
        "enabled"
    } else {
        "disabled"
    };

    let relay_mode = if config.network.relay_mode {
        "enabled"
    } else {
        "disabled"
    };

    format!(
        r"Allow Schema IDs: {}
Database URL: {}
mDNS: {}
Relay Mode: {}

Node is ready!
",
        allow_schema_ids.blue(),
        database_url.blue(),
        mdns.blue(),
        relay_mode.blue(),
    )
}

/// Helper struct to deserialize from either a wildcard string "*" or a list of string values.
///
/// These string values are not checked yet and need to be validated in a succeeding step.
#[derive(Debug, Clone)]
pub enum UncheckedAllowList {
    Wildcard,
    Set(Vec<String>),
}

impl Serialize for UncheckedAllowList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            UncheckedAllowList::Wildcard => serializer.serialize_str(WILDCARD),
            UncheckedAllowList::Set(list) => list.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for UncheckedAllowList {
    fn deserialize<D>(deserializer: D) -> Result<UncheckedAllowList, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value<T> {
            String(String),
            Vec(Vec<T>),
        }

        let value = Value::deserialize(deserializer)?;

        match value {
            Value::String(str_value) => {
                if str_value == WILDCARD {
                    Ok(UncheckedAllowList::Wildcard)
                } else {
                    Err(serde::de::Error::custom("only wildcard strings allowed"))
                }
            }
            Value::Vec(vec) => Ok(UncheckedAllowList::Set(vec)),
        }
    }
}
