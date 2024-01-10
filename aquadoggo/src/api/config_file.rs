// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{anyhow, Result};
use libp2p::PeerId;
use p2panda_rs::schema::SchemaId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tempfile::TempDir;

use crate::{AllowList, Configuration, NetworkConfiguration};

const WILDCARD: &str = "*";

const DEFAULT_LOG_LEVEL: &str = "off";

const DEFAULT_MAX_DATABASE_CONNECTIONS: u32 = 32;

const DEFAULT_HTTP_PORT: u16 = 2020;

const DEFAULT_QUIC_PORT: u16 = 2022;

const DEFAULT_WORKER_POOL_SIZE: u32 = 16;

const DEFAULT_MDNS: bool = true;

static TMP_DIR: OnceLock<TempDir> = OnceLock::new();

fn default_log_level() -> String {
    DEFAULT_LOG_LEVEL.to_string()
}

fn default_max_database_connections() -> u32 {
    DEFAULT_MAX_DATABASE_CONNECTIONS
}

fn default_http_port() -> u16 {
    DEFAULT_HTTP_PORT
}

fn default_quic_port() -> u16 {
    DEFAULT_QUIC_PORT
}

fn default_database_url() -> String {
    // Give each in-memory SQLite database an unique name as we're observing funny issues with
    // SQLite sharing data between processes (!) and breaking each others databases
    // potentially.
    //
    // See related issue: https://github.com/p2panda/aquadoggo/issues/568
    let db_name = format!("dbmem{}", rand::random::<u32>());

    // Set "mode=memory" to enable SQLite running in-memory and set "cache=shared", as
    // setting it to "private" would break sqlx / SQLite.
    //
    // See related issue: https://github.com/launchbadge/sqlx/issues/2510
    format!("sqlite://file:{db_name}?mode=memory&cache=shared")
}

fn default_worker_pool_size() -> u32 {
    DEFAULT_WORKER_POOL_SIZE
}

fn default_mdns() -> bool {
    DEFAULT_MDNS
}

/// Node configuration which can be de/serialized from a config file.
///
/// See https://github.com/p2panda/aquadoggo/blob/main/aquadoggo_cli/config.toml for example
/// config file and detailed documentation of possible configuration values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigFile {
    /// Set log verbosity. Use this for learning more about how your node behaves or for debugging.
    ///
    /// Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE. They are scoped to "aquadoggo" by
    /// default.
    ///
    /// If you want to adjust the scope for deeper inspection use a filter value, for example
    /// "=TRACE" for logging _everything_ or "aquadoggo=INFO,libp2p=DEBUG" etc.
    #[serde(default = "default_log_level")]
    pub log_level: String,

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
    #[serde(default)]
    pub allow_schema_ids: UncheckedAllowList,

    /// URL / connection string to PostgreSQL or SQLite database. Defaults to an in-memory SQLite
    /// database.
    ///
    /// WARNING: By default your node will not persist anything after shutdown. Set a database
    /// connection url for production settings to not loose data.
    #[serde(default = "default_database_url")]
    pub database_url: String,

    /// Max database connections, defaults to 32.
    #[serde(default = "default_max_database_connections")]
    pub database_max_connections: u32,

    /// HTTP port for client-node communication, serving the GraphQL API. Defaults to 2020.
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// QUIC port for node-node communication and data replication. Defaults to 2022.
    #[serde(default = "default_quic_port")]
    pub quic_port: u16,

    /// Path to folder where blobs (large binary files) are persisted. Defaults to a temporary
    /// directory.
    ///
    /// WARNING: By default your node will not persist any blobs after shutdown. Set a path for
    /// production settings to not loose data.
    #[serde(default)]
    pub blobs_base_path: Option<PathBuf>,

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
    #[serde(default)]
    pub private_key: Option<PathBuf>,

    /// mDNS to discover other peers on the local network. Enabled by default.
    #[serde(default = "default_mdns")]
    pub mdns: bool,

    /// List of known node addresses we want to connect to directly.
    ///
    /// Make sure that nodes mentioned in this list are directly reachable (they need to be hosted
    /// with a static IP Address). If you need to connect to nodes with changing, dynamic IP
    /// addresses or even with nodes behind a firewall or NAT, do not use this field but use at
    /// least one relay.
    #[serde(default)]
    pub direct_node_addresses: Vec<SocketAddr>,

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
    #[serde(default)]
    pub allow_peer_ids: UncheckedAllowList,

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
    #[serde(default)]
    pub block_peer_ids: Vec<PeerId>,

    /// List of relay addresses.
    ///
    /// A relay helps discover other nodes on the internet (also known as "rendesvouz" or
    /// "bootstrap" server) and helps establishing direct p2p connections when node is behind a
    /// firewall or NAT (also known as "holepunching").
    ///
    /// WARNING: This will potentially expose your IP address on the network. Do only connect to
    /// trusted relays or make sure your IP address is hidden via a VPN or proxy if you're
    /// concerned about leaking your IP.
    #[serde(default)]
    pub relay_addresses: Vec<SocketAddr>,

    /// Enable if node should also function as a relay. Disabled by default.
    ///
    /// Other nodes can use relays to aid discovery and establishing connectivity.
    ///
    /// Relays _need_ to be hosted in a way where they can be reached directly, for example with a
    /// static IP address through an VPS.
    #[serde(default)]
    pub relay_mode: bool,

    /// Worker pool size, defaults to 16.
    #[serde(default = "default_worker_pool_size")]
    pub worker_pool_size: u32,
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            allow_schema_ids: UncheckedAllowList::default(),
            database_url: default_database_url(),
            database_max_connections: default_max_database_connections(),
            http_port: default_http_port(),
            quic_port: default_quic_port(),
            blobs_base_path: None,
            mdns: default_mdns(),
            private_key: None,
            direct_node_addresses: vec![],
            allow_peer_ids: UncheckedAllowList::default(),
            block_peer_ids: vec![],
            relay_addresses: vec![],
            relay_mode: false,
            worker_pool_size: default_worker_pool_size(),
        }
    }
}

impl TryFrom<ConfigFile> for Configuration {
    type Error = anyhow::Error;

    fn try_from(value: ConfigFile) -> Result<Self, Self::Error> {
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

        // Check if given peer ids are valid
        let allow_peer_ids = match value.allow_peer_ids {
            UncheckedAllowList::Wildcard => AllowList::<PeerId>::Wildcard,
            UncheckedAllowList::Set(str_values) => {
                let peer_ids: Result<Vec<PeerId>, anyhow::Error> = str_values
                    .iter()
                    .map(|str_value| {
                        PeerId::from_str(str_value).map_err(|_| {
                            anyhow!("Invalid peer id '{str_value}' found in 'allow_peer_ids' list")
                        })
                    })
                    .collect();

                AllowList::Set(peer_ids?)
            }
        };

        // Create a temporary blobs directory when none was given
        let blobs_base_path = match value.blobs_base_path {
            Some(path) => path,
            None => TMP_DIR
                .get_or_init(|| {
                    // Initialise a `TempDir` instance globally to make sure it does not run out of
                    // scope and gets deleted before the end of the application runtime
                    tempfile::TempDir::new()
                        .expect("Could not create temporary directory to store blobs")
                })
                .path()
                .to_path_buf(),
        };

        Ok(Configuration {
            allow_schema_ids,
            database_url: value.database_url,
            database_max_connections: value.database_max_connections,
            http_port: value.http_port,
            blobs_base_path,
            worker_pool_size: value.worker_pool_size,
            network: NetworkConfiguration {
                quic_port: value.quic_port,
                mdns: value.mdns,
                direct_node_addresses: value.direct_node_addresses,
                allow_peer_ids,
                block_peer_ids: value.block_peer_ids,
                relay_addresses: value.relay_addresses,
                relay_mode: value.relay_mode,
                ..Default::default()
            },
        })
    }
}

/// Helper struct to deserialize from either a wildcard string "*" or a list of string values.
///
/// These string values are not checked yet and need to be validated in a succeeding step.
#[derive(Debug, Clone, Default)]
pub enum UncheckedAllowList {
    #[default]
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
