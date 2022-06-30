// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use directories::ProjectDirs;
use serde::Deserialize;

use crate::replication::ReplicationConfiguration;

/// Data directory name.
const DATA_DIR_NAME: &str = "aquadoggo";

/// Filename of default sqlite database.
const DEFAULT_SQLITE_NAME: &str = "aquadoggo-node.sqlite3";

/// Configuration object holding all important variables throughout the application.
///
/// Each configuration also assures that a data directory exists on the host machine where database
/// files or private keys get persisted.
///
/// When no custom directory path is set it reads the process environment $XDG_DATA_HOME variable
/// to determine the XDG data directory path which is $HOME/.local/share/aquadoggo on Linux by
/// default.
#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct Configuration {
    /// Path to data directory.
    pub base_path: Option<PathBuf>,

    /// Database url (SQLite or PostgreSQL).
    pub database_url: Option<String>,

    /// Maximum number of database connections in pool.
    pub database_max_connections: u32,

    /// RPC API HTTP server port.
    pub http_port: u16,

    /// RPC API WebSocket server port.
    pub ws_port: u16,

    /// Materializer worker pool size.
    pub worker_pool_size: u32,

    /// Replication configuration.
    pub replication: ReplicationConfiguration,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            base_path: None,
            database_url: None,
            database_max_connections: 32,
            http_port: 2020,
            ws_port: 2022,
            worker_pool_size: 16,
            replication: ReplicationConfiguration::default(),
        }
    }
}

impl Configuration {
    /// Returns the data directory path and creates the folders when not existing.
    fn create_data_directory(path: Option<PathBuf>) -> Result<PathBuf> {
        // Use custom data directory path or determine one from host
        let base_path = path.unwrap_or_else(|| {
            ProjectDirs::from("", "", DATA_DIR_NAME)
                .ok_or("Can not determine data directory")
                .unwrap()
                .data_dir()
                .to_path_buf()
        });

        // Create folders when they don't exist yet
        fs::create_dir_all(&base_path)?;

        Ok(base_path)
    }

    /// Create a new configuration object pulling in the variables from the process environment.
    /// This method also assures a data directory exists on the host machine.
    pub fn new(path: Option<PathBuf>) -> Result<Self> {
        // Make sure data directory exists
        let base_path = Self::create_data_directory(path)?;

        // Create configuration based on defaults and populate with environment variables
        let mut config = envy::from_env::<Self>()?;

        // Store data directory path in object
        config.base_path = Some(base_path);

        // Set default database url (sqlite) when not given
        config.database_url = match config.database_url {
            Some(url) => Some(url),
            None => {
                let mut path = config.base_path.clone().unwrap();
                path.push(DEFAULT_SQLITE_NAME);
                Some(format!("sqlite:{}", path.to_str().unwrap()))
            }
        };

        Ok(config)
    }
}
