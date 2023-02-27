// SPDX-License-Identifier: AGPL-3.0-or-later

use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};

/// Libp2p config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Libp2pConfiguration {
    /// Local address.
    pub listening_multiaddr: Multiaddr,

    /// Mdns discovery enabled.
    pub mdns: bool,

    /// Maximum outgoing connections.
    pub max_connections_out: u32,

    /// Maximum incoming connections.
    pub max_connections_in: u32,

    /// Maximum pending outgoing connections.
    pub max_connections_pending_out: u32,

    /// Maximum pending incoming connections.
    pub max_connections_pending_in: u32,

    /// Maximum connections per peer (includes outgoing and incoming).
    pub max_connections_per_peer: u32,

    /// Notify handler buffer size.
    ///
    /// Defines the buffer size for events sent from the NetworkBehaviour to the ConnectionHandler.
    /// If the buffer is exceeded, the Swarm will have to wait. An individual buffer with this
    /// number of events exists for each individual connection.
    pub notify_handler_buffer_size: usize,

    /// Connection event buffer size.
    ///
    /// Defines the additional buffer size for events sent from the ConnectionHandler to the
    /// NetworkBehaviour. A shared buffer exists with one available "slot" per connection. This
    /// value defines the number of additional slots per connection. If the buffer is exceeded,
    /// the ConnectionHandler will sleep.
    pub connection_event_buffer_size: usize,

    /// Dial concurrency factor.
    ///
    /// Number of addresses concurrently dialed for a single outbound
    /// connection attempt.
    pub dial_concurrency_factor: u8,
}

impl Default for Libp2pConfiguration {
    fn default() -> Self {
        Self {
            // Listen on 127.0.0.1 and a random, OS-assigned port
            listening_multiaddr: "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
            mdns: false,
            max_connections_pending_out: 8,
            max_connections_pending_in: 8,
            max_connections_in: 16,
            max_connections_out: 16,
            max_connections_per_peer: 8,
            notify_handler_buffer_size: 128,
            connection_event_buffer_size: 128,
            dial_concurrency_factor: 8,
        }
    }
}
