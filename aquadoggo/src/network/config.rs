// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use anyhow::Error;
use libp2p::connection_limits::ConnectionLimits;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

use crate::AllowList;

/// The namespace used by the `identify` network behaviour.
pub const NODE_NAMESPACE: &str = "aquadoggo";

/// Network config for the node.
#[derive(Debug, Clone)]
pub struct NetworkConfiguration {
    /// QUIC port for node-node communication and data replication.
    pub quic_port: u16,

    /// Discover peers on the local network via mDNS (over IPv4 only, using port 5353).
    pub mdns: bool,

    /// List of known node addresses we want to connect to directly.
    ///
    /// Make sure that nodes mentioned in this list are directly reachable (they need to be hosted
    /// with a static IP Address). If you need to connect to nodes with changing, dynamic IP
    /// addresses or even with nodes behind a firewall or NAT, do not use this field but use at
    /// least one relay.
    pub direct_node_addresses: Vec<PeerAddress>,

    /// List of peers which are allowed to connect to your node.
    ///
    /// If set then only nodes (identified by their peer id) contained in this list will be able to
    /// connect to your node (via a relay or directly). When not set any other node can connect to
    /// yours.
    ///
    /// Peer IDs identify nodes by using their hashed public keys. They do _not_ represent authored
    /// data from clients and are only used to authenticate nodes towards each other during
    /// networking.
    ///
    /// Use this list for example for setups where the identifier of the nodes you want to form a
    /// network with is known but you still need to use relays as their IP addresses change
    /// dynamically.
    pub allow_peer_ids: AllowList<PeerId>,

    /// List of peers which will be blocked from connecting to your node.
    ///
    /// If set then any peers (identified by their peer id) contained in this list will be blocked
    /// from connecting to your node (via a relay or directly). When an empty list is provided then
    /// there are no restrictions on which nodes can connect to yours.
    ///
    /// Block lists and allow lists are exclusive, which means that you should _either_ use a block
    /// list _or_ an allow list depending on your setup.
    ///
    /// Use this list for example if you want to allow _any_ node to connect to yours _except_ of a
    /// known number of excluded nodes.
    pub block_peer_ids: Vec<PeerId>,

    /// List of relay addresses.
    ///
    /// A relay helps discover other nodes on the internet (also known as "rendesvouz" or
    /// "bootstrap" server) and helps establishing direct p2p connections when node is behind a
    /// firewall or NAT (also known as "holepunching").
    ///
    /// WARNING: This will potentially expose your IP address on the network. Do only connect to
    /// trusted relays or make sure your IP address is hidden via a VPN or proxy if you're
    /// concerned about leaking your IP.
    pub relay_addresses: Vec<PeerAddress>,

    /// Enable if node should also function as a relay.
    ///
    /// Other nodes can use relays to aid discovery and establishing connectivity.
    ///
    /// Relays _need_ to be hosted in a way where they can be reached directly, for example with a
    /// static IP address through an VPS.
    pub relay_mode: bool,

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
            quic_port: 2022,
            mdns: true,
            direct_node_addresses: Vec::new(),
            allow_peer_ids: AllowList::<PeerId>::Wildcard,
            block_peer_ids: Vec::new(),
            relay_addresses: Vec::new(),
            relay_mode: false,
            notify_handler_buffer_size: 128,
            per_connection_event_buffer_size: 8,
            dial_concurrency_factor: 8,
            max_connections_in: 16,
            max_connections_out: 16,
            max_connections_pending_in: 8,
            max_connections_pending_out: 8,
            max_connections_per_peer: 8,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAddress(String);

impl PeerAddress {
    pub fn to_socket(&self) -> Result<SocketAddr, Error> {
        match self.0.to_socket_addrs() {
            Ok(mut addrs) => addrs
                .next()
                .ok_or(anyhow::format_err!("No socket addresses found")),
            Err(e) => Err(e.into()),
        }
    }

    pub fn to_quic_multiaddr(&self) -> Result<Multiaddr, Error> {
        match self.to_socket() {
            Ok(socket_address) => {
                let mut multiaddr = match socket_address.ip() {
                    IpAddr::V4(ip) => Multiaddr::from(Protocol::Ip4(ip)),
                    IpAddr::V6(ip) => Multiaddr::from(Protocol::Ip6(ip)),
                };
                multiaddr.push(Protocol::Udp(socket_address.port()));
                multiaddr.push(Protocol::QuicV1);
                Ok(multiaddr)
            }
            Err(e) => Err(e),
        }
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
