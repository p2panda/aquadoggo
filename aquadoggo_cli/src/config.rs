// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Result};
use aquadoggo::{AllowList, ConfigFile, Configuration};
use clap::{crate_version, Parser};
use colored::Colorize;
use directories::ProjectDirs;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use libp2p::PeerId;
use serde::{Serialize, Serializer};

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
pub fn load_config() -> Result<(ConfigFilePath, ConfigFile)> {
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

    let mut figment = Figment::from(Serialized::defaults(ConfigFile::default()));
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
    name = "aquadoggo",
    about = "Node for the p2panda network",
    long_about = None,
    version
)]
struct Cli {
    /// Path to an optional "config.toml" file for further configuration.
    ///
    /// When not set the program will try to find a `config.toml` file in the same folder the
    /// program is executed in and otherwise in the regarding operation systems XDG config
    /// directory ("$HOME/.config/aquadoggo/config.toml" on Linux).
    #[arg(short = 'c', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<PathBuf>,

    /// List of schema ids which a node will replicate, persist and expose on the GraphQL API.
    /// Separate multiple values with a whitespace. Defaults to allow _any_ schemas ("*").
    ///
    /// When allowing a schema you automatically opt into announcing, replicating and materializing
    /// documents connected to it, supporting applications and networks which are dependent on this
    /// data.
    ///
    /// It is recommended to set this list to all schema ids your own application should support,
    /// including all important system schemas.
    ///
    /// WARNING: When set to wildcard "*", your node will support _any_ schemas it will encounter
    /// on the network. This is useful for experimentation and local development but _not_
    /// recommended for production settings.
    #[arg(short = 's', long, value_name = "SCHEMA_ID", num_args = 0..)]
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_with_wildcard"
    )]
    allow_schema_ids: Option<Vec<String>>,

    /// URL / connection string to PostgreSQL or SQLite database. Defaults to an in-memory SQLite
    /// database.
    ///
    /// WARNING: By default your node will not persist anything after shutdown. Set a database
    /// connection url for production settings to not loose data.
    #[arg(short = 'd', long, value_name = "CONNECTION_STRING")]
    #[serde(skip_serializing_if = "Option::is_none")]
    database_url: Option<String>,

    /// HTTP port for client-node communication, serving the GraphQL API. Defaults to 2020.
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    http_port: Option<u16>,

    /// QUIC port for node-node communication and data replication. Defaults to 2022.
    #[arg(short = 'q', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    quic_port: Option<u16>,

    /// Path to folder where blobs (large binary files) are persisted. Defaults to a temporary
    /// directory.
    ///
    /// WARNING: By default your node will not persist any blobs after shutdown. Set a path for
    /// production settings to not loose data.
    #[arg(short = 'f', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_base_path: Option<PathBuf>,

    /// Path to persist your ed25519 private key file. Defaults to an ephemeral key only for this
    /// current session.
    ///
    /// The key is used to identify you towards other nodes during network discovery and
    /// replication. This key is _not_ used to create and sign data.
    ///
    /// If a path is set, a key will be generated newly and stored under this path when node starts
    /// for the first time.
    ///
    /// When no path is set, your node will generate an ephemeral private key on every start up and
    /// _not_ persist it.
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    /// mDNS to discover other peers on the local network. Enabled by default.
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
    ///
    /// Make sure that nodes mentioned in this list are directly reachable (they need to be hosted
    /// with a static IP Address). If you need to connect to nodes with changing, dynamic IP
    /// addresses or even with nodes behind a firewall or NAT, do not use this field but use at
    /// least one relay.
    #[arg(short = 'n', long, value_name = "IP:PORT", num_args = 0..)]
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_node_addresses: Option<Vec<SocketAddr>>,

    /// List of peers which are allowed to connect to your node.
    ///
    /// If set then only nodes (identified by their peer id) contained in this list will be able to
    /// connect to your node (via a relay or directly). When not set any other node can connect to
    /// yours.
    ///
    /// Peer IDs identify nodes by using their hashed public keys. They do _not_ represent authored
    /// data from clients and are only used to authenticate nodes towards each other during
    /// networking.
    ///
    /// Use this list for example for setups where the identifier of the nodes you want to form a
    /// network with is known but you still need to use relays as their IP addresses change
    /// dynamically.
    #[arg(short = 'a', long, value_name = "PEER_ID", num_args = 0..)]
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_with_wildcard"
    )]
    allow_peer_ids: Option<Vec<String>>,

    /// List of peers which will be blocked from connecting to your node.
    ///
    /// If set then any peers (identified by their peer id) contained in this list will be blocked
    /// from connecting to your node (via a relay or directly). When an empty list is provided then
    /// there are no restrictions on which nodes can connect to yours.
    ///
    /// Block lists and allow lists are exclusive, which means that you should _either_ use a block
    /// list _or_ an allow list depending on your setup.
    ///
    /// Use this list for example if you want to allow _any_ node to connect to yours _except_ of a
    /// known number of excluded nodes.
    #[arg(short = 'b', long, value_name = "PEER_ID", num_args = 0..)]
    #[serde(skip_serializing_if = "Option::is_none")]
    block_peer_ids: Option<Vec<PeerId>>,

    /// List of relay addresses.
    ///
    /// A relay helps discover other nodes on the internet (also known as "rendesvouz" or
    /// "bootstrap" server) and helps establishing direct p2p connections when node is behind a
    /// firewall or NAT (also known as "holepunching").
    ///
    /// WARNING: This will potentially expose your IP address on the network. Do only connect to
    /// trusted relays or make sure your IP address is hidden via a VPN or proxy if you're
    /// concerned about leaking your IP.
    #[arg(short = 'r', long, value_name = "IP:PORT", num_args = 0..)]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_addresses: Option<Vec<SocketAddr>>,

    /// Enable if node should also function as a relay. Disabled by default.
    ///
    /// Other nodes can use relays to aid discovery and establishing connectivity.
    ///
    /// Relays _need_ to be hosted in a way where they can be reached directly, for example with a
    /// static IP address through an VPS.
    #[arg(
        short = 'e',
        long,
        value_name = "BOOL",
        default_missing_value = "true",
        num_args = 0..=1,
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_mode: Option<bool>,

    /// Set log verbosity. Use this for learning more about how your node behaves or for debugging.
    ///
    /// Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE. They are scoped to "aquadoggo" by
    /// default.
    ///
    /// If you want to adjust the scope for deeper inspection use a filter value, for example
    /// "=TRACE" for logging _everything_ or "aquadoggo=INFO,libp2p=DEBUG" etc.
    #[arg(short = 'l', long, value_name = "LEVEL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
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
            if list.len() == 1 && list[0] == WILDCARD {
                // Wildcard symbol comes in form of an array ["*"], convert it to just a string "*"
                serializer.serialize_str(WILDCARD)
            } else if list.len() == 1 && list[0].is_empty() {
                // Empty string should not lead to [""] but to an empty array []
                Vec::<Vec<String>>::new().serialize(serializer)
            } else {
                list.serialize(serializer)
            }
        }
        None => unreachable!("Serialization is skipped if value is None"),
    }
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

pub fn print_config(
    private_key_path: Option<&PathBuf>,
    config_file_path: ConfigFilePath,
    config: &Configuration,
) -> String {
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

    match config_file_path {
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
        AllowList::Wildcard => format!("{WILDCARD} (any schema id)"),
    };

    let database_url = if config.database_url == "sqlite::memory:"
        || config.database_url.contains("mode=memory")
    {
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

    let private_key = match &private_key_path {
        Some(path) => {
            format!("{}", absolute_path(path).display().to_string().blue())
        }
        None => "ephemeral (not persisted)".into(),
    };

    let relay_mode = if config.network.relay_mode {
        "enabled"
    } else {
        "disabled"
    };

    format!(
        r"Allow schema IDs: {}
Database URL: {}
mDNS: {}
Private key: {}
Relay mode: {}

Node is ready!
",
        allow_schema_ids.blue(),
        database_url.blue(),
        mdns.blue(),
        private_key.blue(),
        relay_mode.blue(),
    )
}
