// SPDX-License-Identifier: AGPL-3.0-or-later

use libp2p::connection_limits::ConnectionLimits;
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

/// The namespace used by the `identify` network behaviour.
pub const NODE_NAMESPACE: &str = "aquadoggo";

/// Network config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkConfiguration {
    /// AutoNAT behaviour enabled.
    ///
    /// Determine NAT status by requesting remote peers to dial the public address of the local node.
    pub autonat: bool,

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
    /// Send outbound pings to connected peers every 15 seconds and respond to inbound pings. Every
    /// sent ping must yield a response within 20 seconds in order to be successful.
    pub ping: bool,

    /// QUIC transport port.
    pub quic_port: Option<u16>,

    /// Address and peer ID of a relay server in the form of a multiaddress.
    pub relay_address: Option<Multiaddr>,

    /// Peer ID of a relay server.
    pub relay_peer_id: Option<PeerId>,

    /// Relay server behaviour enabled.
    ///
    /// Serve as a relay point for peer connections.
    pub relay_server_enabled: bool,

    /// The addresses of remote peers to replicate from.
    pub remote_peers: Vec<Multiaddr>,

    /// Address and peer ID of a rendezvous server in the form of a multiaddress.
    pub rendezvous_address: Option<Multiaddr>,

    /// Peer ID of a rendezvous server.
    pub rendezvous_peer_id: Option<PeerId>,

    /// Rendezvous server behaviour enabled.
    ///
    /// Serve as a rendezvous point for peer discovery, allowing peer registration and queries.
    pub rendezvous_server_enabled: bool,
}

impl Default for NetworkConfiguration {
    fn default() -> Self {
        Self {
            autonat: false,
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
            quic_port: None,
            relay_address: None,
            relay_peer_id: None,
            relay_server_enabled: false,
            remote_peers: Vec::new(),
            rendezvous_address: None,
            rendezvous_peer_id: None,
            rendezvous_server_enabled: false,
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
