use anyhow::Result;
use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

/// Data directory name.
const DATA_DIR_NAME: &str = "p2panda";

/// Configuration object holding all important variables throughout the application.
///
/// Each configuration also assures that a data directory exists on the host machine where database
/// files or private keys get persisted.
///
/// When no custom directory path is set it reads the process environment $XDG_DATA_HOME
/// variable to determine the XDG data directory path which is $HOME/.local/share/p2panda on
/// Linux by default.
///
/// An optional config.toml file is read in the data directory and can be used to overwrite the
/// default variables.
#[derive(Deserialize)]
#[serde(default)]
pub struct Configuration {
    /// Path to data directory (can be changed via command line argument)
    pub base_path: Option<PathBuf>,
    /// Maximum number of connections for WebSocket RPC server.
    pub rpc_max_payload: usize,
    /// RPC API HTTP server port.
    pub http_port: u16,
    /// Number of HTTP server threads to run.
    pub http_threads: usize,
    /// Maximal size of RPC request body in bytes.
    pub ws_max_connections: usize,
    /// RPC API WebSocket server port.
    pub ws_port: u16,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            base_path: None,
            rpc_max_payload: 128,
            http_port: 9123,
            http_threads: 4,
            ws_max_connections: 512000,
            ws_port: 9456,
        }
    }
}

impl Configuration {
    /// Returns the data directory path and creates the folders when not existing.
    fn create_data_directory(path: Option<PathBuf>) -> Result<PathBuf> {
        // Use custom data directory path or determine one from host
        let base_path = path.unwrap_or(
            ProjectDirs::from("", "", DATA_DIR_NAME)
                .ok_or("Can not determine data directory")
                .unwrap()
                .data_dir()
                .to_path_buf(),
        );

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

        Ok(config)
    }
}
