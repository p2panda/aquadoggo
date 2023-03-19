// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::swarm::ConnectionLimits;
use libp2p::{Multiaddr, PeerId};
use log::info;
use serde::{Deserialize, Serialize};

use crate::network::identity::Identity;

/// Key pair file name.
const KEY_PAIR_FILE_NAME: &str = "private-key";

// TODO: make the namespace dynamic (ie. can be user-assigned)
/// The namespace used by the `identify` network behaviour.
pub const NODE_NAMESPACE: &str = "aquadoggo";

/// Network config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkConfiguration {
    /// Dial concurrency factor.
    ///
    /// Number of addresses concurrently dialed for an outbound connection attempt with a single
    /// peer.
    pub dial_concurrency_factor: u8,

    /// Maximum incoming connections.
    pub max_connections_in: u32,

    /// Maximum outgoing connections.
    pub max_connections_out: u32,

    /// Maximum pending incoming connections.
    ///
    /// A pending connection is one which has been initiated but has not yet received a response.
    pub max_connections_pending_in: u32,

    /// Maximum pending outgoing connections.
    ///
    /// A pending connection is one which has been initiated but has not yet received a response.
    pub max_connections_pending_out: u32,

    /// Maximum connections per peer (includes outgoing and incoming).
    pub max_connections_per_peer: u32,

    /// mDNS discovery enabled.
    ///
    /// Automatically discover peers on the local network (over IPv4 only, using port 5353).
    pub mdns: bool,

    /// Notify handler buffer size.
    ///
    /// Defines the buffer size for events sent from a network protocol handler to the connection
    /// manager. If the buffer is exceeded, other network processes will have to wait while the
    /// events are processed. An individual buffer with this number of events exists for each
    /// individual connection.
    pub notify_handler_buffer_size: usize,

    /// Connection event buffer size.
    ///
    /// Defines the buffer size for events sent from the connection manager to a network protocol
    /// handler. Each connection has its own buffer. If the buffer is exceeded, the connection
    /// manager will sleep.
    pub per_connection_event_buffer_size: usize,

    /// Ping behaviour enabled.
    ///
    /// Send outbound pings to connected peers every 15 seconds and respond to inbound pings.
    /// Every sent ping must yield a response within 20 seconds in order to be successful.
    pub ping: bool,

    /// QUIC transport port.
    pub quic_port: u16,

    /// The addresses of remote peers to replicate from.
    pub remote_peers: Vec<Multiaddr>,

    /// Rendezvous client behaviour enabled.
    ///
    /// Connect to a rendezvous point, register the local node and query addresses of remote peers.
    pub rendezvous_client: bool,

    /// Rendezvous server behaviour enabled.
    ///
    /// Serve as a rendezvous point for peer discovery, allowing peer registration and queries.
    pub rendezvous_server: bool,

    /// Address of a rendezvous server in the form of a multiaddress.
    pub rendezvous_address: Option<Multiaddr>,

    /// Peer ID of a rendezvous server.
    pub rendezvous_peer_id: Option<PeerId>,
}

impl Default for NetworkConfiguration {
    fn default() -> Self {
        Self {
            dial_concurrency_factor: 8,
            max_connections_in: 16,
            max_connections_out: 16,
            max_connections_pending_in: 8,
            max_connections_pending_out: 8,
            max_connections_per_peer: 8,
            mdns: false,
            notify_handler_buffer_size: 128,
            per_connection_event_buffer_size: 8,
            ping: false,
            quic_port: 2022,
            remote_peers: Vec::new(),
            rendezvous_client: false,
            rendezvous_server: false,
            rendezvous_address: None,
            rendezvous_peer_id: None,
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
