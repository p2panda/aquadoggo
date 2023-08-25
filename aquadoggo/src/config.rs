// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::SchemaId;

use crate::network::NetworkConfiguration;

/// Configuration object holding all important variables throughout the application.
#[derive(Debug, Clone)]
pub struct Configuration {
    /// List of schema ids which a node will replicate, persist and expose on the GraphQL API.
    ///
    /// When allowing a schema you automatically opt into announcing, replicating and materializing
    /// documents connected to it, supporting applications and networks which are dependent on this
    /// data.
    ///
    /// It is recommended to set this list to all schema ids your own application should support,
    /// including all important system schemas.
    ///
    /// **Warning**: When set to `AllowList::Wildcard`, your node will support _any_ schemas it
    /// will encounter on the network. This is useful for experimentation and local development but
    /// _not_ recommended for production settings.
    pub allow_schema_ids: AllowList<SchemaId>,

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

    /// Network configuration.
    pub network: NetworkConfiguration,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            allow_schema_ids: AllowList::Wildcard,
            database_url: "sqlite::memory:".into(),
            database_max_connections: 32,
            http_port: 2020,
            worker_pool_size: 16,
            network: NetworkConfiguration::default(),
        }
    }
}

/// Set a configuration value to either allow a defined set of elements or to a wildcard (*).
#[derive(Debug, Clone)]
pub enum AllowList<T> {
    /// Allow all possible items.
    Wildcard,

    /// Allow only a certain set of items.
    Set(Vec<T>),
}

impl<T> Default for AllowList<T> {
    fn default() -> Self {
        Self::Wildcard
    }
}
