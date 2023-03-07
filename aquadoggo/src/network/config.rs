// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::swarm::ConnectionLimits;
use libp2p::Multiaddr;
use log::info;
use serde::{Deserialize, Serialize};

use crate::network::identity::Identity;

/// Key pair file name.
const KEY_PAIR_FILE_NAME: &str = "private-key";

/// QUIC default transport port.
const QUIC_PORT: u16 = 2022;

/// Network config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkConfiguration {
    /// Dial concurrency factor.
    ///
    /// Number of addresses concurrently dialed for a single outbound
    /// connection attempt.
    pub dial_concurrency_factor: u8,

    /// Local address.
    pub listening_multiaddr: Multiaddr,

    /// Maximum incoming connections.
    pub max_connections_in: u32,

    /// Maximum outgoing connections.
    pub max_connections_out: u32,

    /// Maximum pending incoming connections.
    pub max_connections_pending_in: u32,

    /// Maximum pending outgoing connections.
    pub max_connections_pending_out: u32,

    /// Maximum connections per peer (includes outgoing and incoming).
    pub max_connections_per_peer: u32,

    /// mDNS discovery enabled.
    pub mdns: bool,

    /// Notify handler buffer size.
    ///
    /// Defines the buffer size for events sent from the NetworkBehaviour to the ConnectionHandler.
    /// If the buffer is exceeded, the Swarm will have to wait. An individual buffer with this
    /// number of events exists for each individual connection.
    pub notify_handler_buffer_size: usize,

    /// Connection event buffer size.
    ///
    /// Defines the buffer size for events sent from the ConnectionHandler to the
    /// NetworkBehaviour. Each connection has its own buffer. If the buffer is
    /// exceeded, the ConnectionHandler will sleep.
    pub per_connection_event_buffer_size: usize,

    /// Ping behaviour enabled.
    pub ping: bool,
}

impl Default for NetworkConfiguration {
    fn default() -> Self {
        // Define the default listening multiaddress using the default QUIC port
        let listening_multiaddr = format!("/ip4/0.0.0.0/udp/{QUIC_PORT}/quic-v1")
            .parse()
            .unwrap();

        Self {
            dial_concurrency_factor: 8,
            listening_multiaddr,
            max_connections_in: 16,
            max_connections_out: 16,
            max_connections_pending_in: 8,
            max_connections_pending_out: 8,
            max_connections_per_peer: 8,
            mdns: false,
            notify_handler_buffer_size: 128,
            per_connection_event_buffer_size: 8,
            ping: false,
        }
    }
}

impl NetworkConfiguration {
    /// Define the connection limits of the swarm.
    pub fn connection_limits(&self) -> ConnectionLimits {
        ConnectionLimits::default()
            .with_max_pending_outgoing(Some(self.max_connections_pending_out))
            .with_max_pending_incoming(Some(self.max_connections_pending_in))
            .with_max_established_outgoing(Some(self.max_connections_out))
            .with_max_established_incoming(Some(self.max_connections_in))
            .with_max_established_per_peer(Some(self.max_connections_per_peer))
    }

    /// Load the key pair from the file at the specified path.
    ///
    /// If the file does not exist, a random key pair is generated and saved.
    /// If no path is specified, a random key pair is generated.
    pub fn load_or_generate_key_pair(path: Option<PathBuf>) -> Result<Keypair> {
        let key_pair = match path {
            Some(mut path) => {
                // Extend the base data directory path with the key pair filename
                path.push(KEY_PAIR_FILE_NAME);

                // Check if the key pair file exists.
                // If not, generate a new key pair and write it to file
                if !path.is_file() {
                    let identity: Keypair = Identity::new();
                    identity.save(&path)?;
                    info!("Created new network key pair and saved it to {:?}", path);
                    identity.key_pair()
                } else {
                    // If the key pair file exists, open it and load the key pair
                    let stored_identity: Keypair = Identity::load(&path)?;
                    stored_identity.key_pair()
                }
            }
            None => {
                // No path was provided. Generate and return a random key pair
                Identity::new()
            }
        };

        Ok(key_pair)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::NetworkConfiguration;

    #[test]
    fn generates_new_key_pair() {
        let key_pair = NetworkConfiguration::load_or_generate_key_pair(None);
        assert!(key_pair.is_ok());
    }

    #[test]
    fn saves_and_loads_key_pair() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp_path = tmp_dir.path().to_owned();

        // Attempt to load the key pair from the temporary path
        // This should result in a new key pair being generated and written to file
        let key_pair_1 = NetworkConfiguration::load_or_generate_key_pair(Some(tmp_path.clone()));
        assert!(key_pair_1.is_ok());

        // Attempt to load the key pair from the same temporary path
        // This should result in the previously-generated key pair being loaded from file
        let key_pair_2 = NetworkConfiguration::load_or_generate_key_pair(Some(tmp_path));
        assert!(key_pair_2.is_ok());

        // Ensure that both key pairs have the same public key
        assert_eq!(key_pair_1.unwrap().public(), key_pair_2.unwrap().public());
    }
}