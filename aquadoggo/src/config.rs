// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use anyhow::bail;
use p2panda_rs::schema::SchemaId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::network::NetworkConfiguration;

/// Configuration object holding all important variables throughout the application.
#[derive(Debug, Clone)]
pub struct Configuration {
    /// URL / connection string to PostgreSQL or SQLite database.
    pub database_url: String,

    /// Maximum number of connections that the database pool should maintain.
    ///
    /// Be mindful of the connection limits for the database as well as other applications which
    /// may want to connect to the same database (or even multiple instances of the same
    /// application in high-availability deployments).
    pub database_max_connections: u32,

    /// HTTP port, serving the GraphQL API (for example hosted under
    /// http://localhost:2020/graphql). This API is used for client-node communication. Defaults to
    /// 2020.
    pub http_port: u16,

    /// Number of concurrent workers which defines the maximum of materialization tasks which can
    /// be worked on simultaneously.
    ///
    /// Use a higher number if you run your node on a powerful machine with many CPU cores. Lower
    /// number for low-energy devices with limited resources.
    pub worker_pool_size: u32,

    /// List of schema ids which a node will replicate and expose on the GraphQL API.
    ///
    /// When allowing a schema you automatically opt into announcing, replicating and materializing
    /// documents connected to it, supporting applications which are dependent on this data.
    pub supported_schema_ids: AllowList<SchemaId>,

    /// Network configuration.
    pub network: NetworkConfiguration,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            database_url: "sqlite::memory:".into(),
            database_max_connections: 32,
            http_port: 2020,
            worker_pool_size: 16,
            supported_schema_ids: AllowList::Wildcard,
            network: NetworkConfiguration::default(),
        }
    }
}

const WILDCARD: &'static str = "*";

/// Set a configuration value to either allow a defined set of elements or to a wildcard (*).
#[derive(Debug, Clone)]
pub enum AllowList<T> {
    /// Allow all possible items.
    Wildcard,

    /// Allow only a certain set of items.
    Set(Vec<T>),
}

impl<T> FromStr for AllowList<T> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == WILDCARD {
            Ok(Self::Wildcard)
        } else {
            bail!("only wildcard strings allowed")
        }
    }
}

impl<T> Default for AllowList<T> {
    fn default() -> Self {
        Self::Wildcard
    }
}

impl<T> Serialize for AllowList<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AllowList::Wildcard => serializer.serialize_str(WILDCARD),
            AllowList::Set(list) => list.serialize(serializer),
        }
    }
}

impl<'de, T> Deserialize<'de> for AllowList<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<AllowList<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
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
                    Ok(AllowList::Wildcard)
                } else {
                    Err(serde::de::Error::custom("only wildcard strings allowed"))
                }
            }
            Value::Vec(vec) => Ok(AllowList::Set(vec)),
        }
    }
}
