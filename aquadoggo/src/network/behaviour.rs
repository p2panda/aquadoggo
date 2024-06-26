// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::Duration;

use anyhow::Result;
use libp2p::allow_block_list;
use libp2p::allow_block_list::{AllowedPeers, BlockedPeers};
use libp2p::identity::Keypair;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{connection_limits, dcutr, identify, mdns, relay, rendezvous};
use log::debug;

use crate::network::config::NODE_NAMESPACE;
use crate::network::peers;
use crate::network::NetworkConfiguration;
use crate::AllowList;

/// How often do we broadcast mDNS queries into the network.
const MDNS_QUERY_INTERVAL: Duration = Duration::from_secs(5);

/// The reservation of a relayed connection becomes invalid after this time and it's the
/// responsibility of the client to refresh.
const RELAY_RESERVATION_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

/// A successfully established, relayed connection becomes invalid after this time and it's the
/// responsibility of the client to refresh.
const RELAY_MAX_CIRCUIT_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

/// Network behaviour for the aquadoggo node.
///
/// In libp2p all different behaviours are "merged" into one "main behaviour" with help of the
/// `NetworkBehaviour` derive macro.
///
/// All behaviours share the same connections with each other. Together they form something we
/// could call our "custom" networking behaviour.
///
/// It is possible for a peer to not support all behaviours, internally libp2p negotiates the
/// capabilities of each peer for us and upgrades the protocol accordingly. For example two peers
/// can handle p2panda messages with each others (using the `peers` behaviour) but do not
/// necessarily need to be able to support the `relay` behaviour.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "Event", event_process = false)]
pub struct P2pandaBehaviour {
    /// Periodically exchange information between peer on an established connection. This is useful
    /// for learning the external address of the local node from a remote peer.
    pub identify: Toggle<identify::Behaviour>,

    /// Enforce a set of connection limits.
    pub limits: connection_limits::Behaviour,

    /// Automatically discover peers on the local network via multicast DNS.
    pub mdns: Toggle<mdns::tokio::Behaviour>,

    /// Communicate with remote peers via a relay server when a direct peer-to-peer connection is
    /// not possible.
    pub relay_client: Toggle<relay::client::Behaviour>,

    /// Serve as a relay point for remote peers to establish connectivity when a direct
    /// peer-to-peer connection is not possible first.
    pub relay_server: Toggle<relay::Behaviour>,

    /// Register with a rendezvous server and query remote peer addresses.
    pub rendezvous_client: Toggle<rendezvous::client::Behaviour>,

    /// Serve as a rendezvous point for remote peers to register their external addresses and query
    /// the addresses of other peers.
    pub rendezvous_server: Toggle<rendezvous::server::Behaviour>,

    /// Allow two peers behind NAT to communicate directly by utilizing a technique called hole
    /// punching.
    ///
    /// The technique relies on the two peers synchronizing and simultaneously opening connections
    /// to each other to their predicted external address with help of a third-party relay server.
    pub dcutr: Toggle<dcutr::Behaviour>,

    /// Allow connections based on an allow list of peer ids.
    pub allowed_peers: Toggle<allow_block_list::Behaviour<AllowedPeers>>,

    /// Block connections based on a block list of peer ids.
    pub blocked_peers: Toggle<allow_block_list::Behaviour<BlockedPeers>>,

    /// Register peer connections and handle p2panda messaging with them.
    pub peers: peers::Behaviour,
}

impl P2pandaBehaviour {
    /// Generate a new instance of the composed network behaviour according to the network
    /// configuration.
    pub fn new(
        network_config: &NetworkConfiguration,
        key_pair: &Keypair,
        relay_client: Option<relay::client::Behaviour>,
    ) -> Result<Self> {
        let peer_id = key_pair.public().to_peer_id();

        // Create an identify server behaviour with default configuration if a rendezvous server
        // address has been provided or the rendezvous server flag is set
        let identify = if !network_config.relay_addresses.is_empty() || network_config.relay_mode {
            debug!("Identify network behaviour enabled");
            Some(identify::Behaviour::new(identify::Config::new(
                format!("{NODE_NAMESPACE}/1.0.0"),
                key_pair.public(),
            )))
        } else {
            None
        };

        // Create an mDNS behaviour with default configuration if the mDNS flag is set
        let mdns = if network_config.mdns {
            debug!("mDNS network behaviour enabled");
            Some(mdns::Behaviour::new(
                mdns::Config {
                    query_interval: MDNS_QUERY_INTERVAL,
                    ..mdns::Config::default()
                },
                peer_id,
            )?)
        } else {
            None
        };

        // Create a limit behaviour with default configuration.
        let limits = connection_limits::Behaviour::new(network_config.connection_limits());

        // Create a rendezvous client behaviour with default configuration if a rendezvous server
        // address has been provided
        let rendezvous_client = if !network_config.relay_addresses.is_empty() {
            debug!("Rendezvous client network behaviour enabled");
            Some(rendezvous::client::Behaviour::new(key_pair.clone()))
        } else {
            None
        };

        // Create a rendezvous server behaviour with default configuration if the rendezvous server
        // flag is set
        let rendezvous_server = if network_config.relay_mode {
            debug!("Rendezvous server network behaviour enabled");
            Some(rendezvous::server::Behaviour::new(
                rendezvous::server::Config::default(),
            ))
        } else {
            None
        };

        if relay_client.is_some() {
            debug!("Relay client network behaviour enabled");
        }

        // Create a relay server behaviour with default configuration if the relay server flag is
        // set
        let relay_server = if network_config.relay_mode {
            debug!("Relay server network behaviour enabled");
            Some(relay::Behaviour::new(
                peer_id,
                relay::Config {
                    max_circuit_duration: RELAY_MAX_CIRCUIT_DURATION,
                    reservation_duration: RELAY_RESERVATION_DURATION,
                    ..relay::Config::default()
                },
            ))
        } else {
            None
        };

        // Create UDP holepunching behaviour (DCUtR) if the flag is set
        let dcutr = if network_config.relay_mode || relay_client.is_some() {
            Some(dcutr::Behaviour::new(peer_id))
        } else {
            None
        };

        // Construct behaviour to manage an allow list of peers when configured
        let allowed_peers = match &network_config.allow_peer_ids {
            AllowList::Wildcard => None,
            AllowList::Set(allow_peer_ids) => {
                let mut allowed_peers = allow_block_list::Behaviour::default();
                for peer_id in allow_peer_ids {
                    allowed_peers.allow_peer(*peer_id)
                }
                Some(allowed_peers)
            }
        };

        // Construct behaviour to manage a block list of peers when configured
        let blocked_peers = if network_config.block_peer_ids.is_empty() {
            None
        } else {
            let mut blocked_peers = allow_block_list::Behaviour::default();
            for peer_id in &network_config.block_peer_ids {
                blocked_peers.block_peer(*peer_id)
            }
            Some(blocked_peers)
        };

        // Always create behaviour to manage peer connections and handle p2panda messaging
        let peers = peers::Behaviour::new();

        Ok(Self {
            identify: identify.into(),
            mdns: mdns.into(),
            limits,
            rendezvous_client: rendezvous_client.into(),
            rendezvous_server: rendezvous_server.into(),
            relay_client: relay_client.into(),
            relay_server: relay_server.into(),
            dcutr: dcutr.into(),
            peers,
            allowed_peers: allowed_peers.into(),
            blocked_peers: blocked_peers.into(),
        })
    }
}

/// Gather all libp2p event types coming from different behaviours into one enum.
#[derive(Debug)]
pub enum Event {
    Identify(identify::Event),
    Mdns(mdns::Event),
    RelayClient(relay::client::Event),
    #[allow(dead_code)]
    RelayServer(relay::Event),
    RendezvousClient(rendezvous::client::Event),
    #[allow(dead_code)]
    RendezvousServer(rendezvous::server::Event),
    Dcutr(dcutr::Event),
    Peers(peers::Event),
    Void,
}

impl From<identify::Event> for Event {
    fn from(e: identify::Event) -> Self {
        Event::Identify(e)
    }
}

impl From<mdns::Event> for Event {
    fn from(e: mdns::Event) -> Self {
        Event::Mdns(e)
    }
}

impl From<relay::client::Event> for Event {
    fn from(e: relay::client::Event) -> Self {
        Event::RelayClient(e)
    }
}

impl From<relay::Event> for Event {
    fn from(e: relay::Event) -> Self {
        Event::RelayServer(e)
    }
}

impl From<rendezvous::client::Event> for Event {
    fn from(e: rendezvous::client::Event) -> Self {
        Event::RendezvousClient(e)
    }
}

impl From<rendezvous::server::Event> for Event {
    fn from(e: rendezvous::server::Event) -> Self {
        Event::RendezvousServer(e)
    }
}

impl From<dcutr::Event> for Event {
    fn from(e: dcutr::Event) -> Self {
        Event::Dcutr(e)
    }
}

impl From<peers::Event> for Event {
    fn from(e: peers::Event) -> Self {
        Event::Peers(e)
    }
}

impl From<void::Void> for Event {
    fn from(_: void::Void) -> Self {
        Event::Void
    }
}
