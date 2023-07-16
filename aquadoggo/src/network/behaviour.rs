// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::Duration;

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, connection_limits, identify, mdns, ping, relay, rendezvous};
use log::debug;

use crate::network::config::NODE_NAMESPACE;
use crate::network::NetworkConfiguration;
use crate::network::{dialer, peers};

/// How often do we broadcast mDNS queries into the network.
const MDNS_QUERY_INTERVAL: Duration = Duration::from_secs(5);

/// How often do we ping other peers to check for a healthy connection.
const PING_INTERVAL: Duration = Duration::from_secs(5);

/// How long do we wait for an answer from the other peer before we consider the connection as
/// stale.
const PING_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(NetworkBehaviour)]
pub struct RelayBehaviour {
    /// Periodically exchange information between peer on an established connection. This
    /// is useful for learning the external address of the local node from a remote peer.
    pub identify: identify::Behaviour,

    /// Enforce a set of connection limits.
    pub limits: connection_limits::Behaviour,

    /// Serve as a relay point for remote peers to establish connectivity when a direct
    /// peer-to-peer connection is not possible.
    pub relay_server: relay::Behaviour,

    /// Serve as a rendezvous point for remote peers to register their external addresses and query
    /// the addresses of other peers.
    pub rendezvous_server: rendezvous::server::Behaviour,

    pub ping: ping::Behaviour,
}

impl RelayBehaviour {
    /// Generate a new instance of the composed network behaviour according to
    /// the network configuration context.
    pub fn new(network_config: &NetworkConfiguration, key_pair: Keypair) -> Result<Self> {
        let peer_id = key_pair.public().to_peer_id();

        let identify = identify::Behaviour::new(identify::Config::new(
            format!("{NODE_NAMESPACE}/1.0.0"),
            key_pair.public(),
        ));

        // Create a limit behaviour with default configuration.
        let limits = connection_limits::Behaviour::new(network_config.connection_limits());

        // Create a rendezvous server behaviour with default configuration if the rendezvous server
        // flag is set
        let rendezvous_server =
            rendezvous::server::Behaviour::new(rendezvous::server::Config::default());

        // Create a relay server behaviour with default configuration if the relay server
        // flag is set
        let relay_server = relay::Behaviour::new(peer_id, relay::Config::default());

        Ok(Self {
            identify,
            limits,
            rendezvous_server,
            relay_server,
            ping: ping::Behaviour::new(ping::Config::default()),
        })
    }
}

#[derive(NetworkBehaviour)]
pub struct ClientBehaviour {
    /// Periodically exchange information between peer on an established connection. This
    /// is useful for learning the external address of the local node from a remote peer.
    pub identify: identify::Behaviour,

    /// Enforce a set of connection limits.
    pub limits: connection_limits::Behaviour,

    /// Communicate with remote peers via a relay server when a direct peer-to-peer
    /// connection is not possible.
    pub relay_client: relay::client::Behaviour,

    /// Register with a rendezvous server and query remote peer addresses.
    pub rendezvous_client: rendezvous::client::Behaviour,

    pub ping: ping::Behaviour,
}

impl ClientBehaviour {
    /// Generate a new instance of the composed network behaviour according to
    /// the network configuration context.
    pub fn new(
        network_config: &NetworkConfiguration,
        key_pair: Keypair,
        relay_client: relay::client::Behaviour,
    ) -> Result<Self> {
        let peer_id = key_pair.public().to_peer_id();

        let identify = identify::Behaviour::new(identify::Config::new(
            format!("{NODE_NAMESPACE}/1.0.0"),
            key_pair.public(),
        ));

        // Create a limit behaviour with default configuration.
        let limits = connection_limits::Behaviour::new(network_config.connection_limits());

        // Create a rendezvous client behaviour with default configuration if a rendezvous server
        // address has been provided
        let rendezvous_client = rendezvous::client::Behaviour::new(key_pair);

        Ok(Self {
            identify,
            limits,
            rendezvous_client,
            relay_client,
            ping: ping::Behaviour::new(ping::Config::default()),
        })
    }
}
