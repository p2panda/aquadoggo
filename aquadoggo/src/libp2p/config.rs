// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::Multiaddr;
use log::info;
use serde::{Deserialize, Serialize};

use crate::libp2p::identity::Identity;

/// Keypair file name.
const KEYPAIR_FILE_NAME: &str = "libp2p.pem";

/// Libp2p config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Libp2pConfiguration {
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

impl Default for Libp2pConfiguration {
    fn default() -> Self {
        Self {
            dial_concurrency_factor: 8,
            // Listen on 127.0.0.1 and a random, OS-assigned port
            listening_multiaddr: "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
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

impl Libp2pConfiguration {
    /// Load the keypair from the file at the specified path.
    /// If the file does not exist, a random keypair is generated and saved.
    /// If no path is specified, a random keypair is generated.
    pub fn load_or_generate_keypair(path: Option<PathBuf>) -> Result<Keypair> {
        let keypair = match path {
            Some(mut path) => {
                // Extend the base data directory path with the keypair filename
                path.push(KEYPAIR_FILE_NAME);

                // Check if the keypair file exists.
                // If not, generate a new keypair and write it to file
                if !path.is_file() {
                    let identity: Keypair = Identity::new();
                    identity.save(&path)?;
                    info!("Created new libp2p keypair and saved it to {:?}", path);
                    identity.keypair()
                } else {
                    // If the keypair file exists, open it and load the keypair
                    let stored_identity: Keypair = Identity::load(&path)?;
                    stored_identity.keypair()
                }
            }
            None => {
                // No path was provided. Generate and return a random keypair
                Identity::new()
            }
        };

        Ok(keypair)
    }
}

#[cfg(test)]
mod tests {
    use super::Libp2pConfiguration;
    use tempfile::TempDir;

    #[test]
    fn generates_new_keypair() {
        let keypair = Libp2pConfiguration::load_or_generate_keypair(None);
        assert!(keypair.is_ok());
    }

    #[test]
    fn saves_and_loads_keypair() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp_path = tmp_dir.path().to_owned();

        // Attempt to load the keypair from the temporary path
        // This should result in a new keypair being generated and written to file
        let keypair_1 = Libp2pConfiguration::load_or_generate_keypair(Some(tmp_path.clone()));
        assert!(keypair_1.is_ok());

        // Attempt to load the keypair from the same temporary path
        // This should result in the previously-generated keypair being loaded from file
        let keypair_2 = Libp2pConfiguration::load_or_generate_keypair(Some(tmp_path));
        assert!(keypair_2.is_ok());

        // Ensure that both keypairs have the same public key
        assert_eq!(keypair_1.unwrap().public(), keypair_2.unwrap().public());
    }
}
