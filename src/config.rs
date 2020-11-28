use config::{Config, ConfigError, Environment, File, FileFormat};
use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

/// XDG data directory prefix.
const CONFIG_NAMESPACE: &str = "p2panda";

/// Config file name inside of data directory.
const CONFIG_FILE_NAME: &str = "config.toml";

/// Maximal payload accepted by RPC servers.
const DEFAULT_MAX_RPC_PAYLOAD: usize = 15 * 1024 * 1024;

/// RPC API HTTP server port.
const DEFAULT_HTTP_PORT: u16 = 9123;

/// Maximum HTTP server threads.
const DEFAULT_HTTP_THREADS: usize = 4;

/// RPC API WebSocket server port.
const DEFAULT_WEBSOCKET_PORT: u16 = 9456;

/// Default maximum number of connections for WebSocket RPC server.
const DEFAULT_WEBSOCKET_CONNECTIONS: usize = 128;

#[derive(Debug, Deserialize)]
pub struct Server {
    pub http_port: u16,
    pub http_threads: usize,
    pub max_payload: usize,
    pub ws_max_connections: usize,
    pub ws_port: u16,
}

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
#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub server: Server,
}

impl Configuration {
    /// Returns the data directory path and creates the folders when not existing.
    fn create_data_directory(path: Option<PathBuf>) -> std::io::Result<PathBuf> {
        // Use custom data directory path or determine one from host
        let base_path = path.unwrap_or(
            ProjectDirs::from("", "", CONFIG_NAMESPACE)
                .ok_or("Can not determine XDG data directory")
                .unwrap()
                .data_dir()
                .to_path_buf(),
        );

        // Create folders when they don't exist yet
        fs::create_dir_all(&base_path)?;

        Ok(base_path)
    }

    /// Create a new configuration object pointing at a data directory where we also check if an
    /// optional config.toml file exists for custom variables.
    pub fn new(path: Option<PathBuf>) -> Result<Self, ConfigError> {
        // Get path of data directory
        let base_path =
            Self::create_data_directory(path).map_err(|err| ConfigError::Foreign(Box::new(err)))?;

        // Create a default configuration
        let mut config = Config::new();
        config.set_default("server.http_port", DEFAULT_HTTP_PORT as i64)?;
        config.set_default("server.http_threads", DEFAULT_HTTP_THREADS as i64)?;
        config.set_default("server.max_payload", DEFAULT_MAX_RPC_PAYLOAD as i64)?;
        config.set_default("server.ws_max_connections", DEFAULT_WEBSOCKET_CONNECTIONS as i64)?;
        config.set_default("server.ws_port", DEFAULT_WEBSOCKET_PORT as i64)?;

        // Read local config file and update variables
        let config_path = base_path.join(CONFIG_FILE_NAME);
        config.merge(File::new(config_path.to_str().unwrap(), FileFormat::Toml).required(false))?;

        // Read process environment and update variables
        config.merge(Environment::with_prefix(CONFIG_NAMESPACE))?;

        config.try_into()
    }
}
