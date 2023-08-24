// SPDX-License-Identifier: AGPL-3.0-or-later

use libp2p::connection_limits::ConnectionLimits;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};

/// The namespace used by the `identify` network behaviour.
pub const NODE_NAMESPACE: &str = "aquadoggo";

/// Network config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkConfiguration {
    /// QUIC port for node-to-node communication.
    pub quic_port: u16,

    /// Discover peers on the local network via mDNS (over IPv4 only, using port 5353).
    pub mdns: bool,

    /// List of known node addresses (IP + port) we want to connect to directly.
    ///
    /// Make sure that nodes mentioned in this list are directly reachable (for example they need
    /// to be hosted with a static IP Address). If you need to connect to nodes with changing,
    /// dynamic IP addresses or even with nodes behind a firewall or NAT, do not use this field but
    /// use at least one relay.
    pub direct_node_addresses: Vec<Multiaddr>,

    /// Set to true if node should also function as a relay. Other nodes can use relays to aid
    /// discovery and establishing connectivity.
    ///
    /// Relays _need_ to be hosted in a way where they can be reached directly, for example with a
    /// static IP address through an VPS.
    pub im_a_relay: bool,

    /// Address of a peer which can act as a relay/rendezvous server.
    ///
    /// Relays help discover other nodes on the internet (also known as "rendesvouz" or "bootstrap"
    /// server) and help establishing direct p2p connections when node is behind a firewall or NAT
    /// (also known as "holepunching").
    ///
    /// When a direct connection is not possible the relay will help to redirect the (encrypted)
    /// traffic as an intermediary between us and other nodes. The node will contact each server
    /// and register our IP address for other peers.
    pub relay_address: Option<Multiaddr>,

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
            mdns: true,
            direct_node_addresses: Vec::new(),
            notify_handler_buffer_size: 128,
            per_connection_event_buffer_size: 8,
            quic_port: 2022,
            im_a_relay: false,
            relay_address: None,
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
}
