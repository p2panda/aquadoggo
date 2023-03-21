// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{identify, mdns, ping, relay, rendezvous, PeerId};
use log::debug;

use crate::network::config::NODE_NAMESPACE;
use crate::network::NetworkConfiguration;

/// Network behaviour for the aquadoggo node.
#[derive(NetworkBehaviour)]
pub struct Behaviour {
    /// Automatically discover peers on the local network.
    pub mdns: Toggle<mdns::tokio::Behaviour>,

    /// Respond to inbound pings and periodically send outbound ping on every established
    /// connection.
    pub ping: Toggle<ping::Behaviour>,

    /// Serve as a relay point for remote peers to establish connectivity when a direct
    /// peer-to-peer connection is not possible.
    pub relay_server: Toggle<relay::Behaviour>,

    /// Register with a rendezvous server and query remote peer addresses.
    pub rendezvous_client: Toggle<rendezvous::client::Behaviour>,

    /// Serve as a rendezvous point for remote peers to register their external addresses
    /// and query the addresses of other peers.
    pub rendezvous_server: Toggle<rendezvous::server::Behaviour>,

    /// Periodically exchange information between peer on an established connection. This
    /// is useful for learning the external address of the local node from a remote peer.
    pub identify: Toggle<identify::Behaviour>,
}

impl Behaviour {
    /// Generate a new instance of the composed network behaviour according to
    /// the network configuration context.
    pub fn new(
        network_config: &NetworkConfiguration,
        peer_id: PeerId,
        key_pair: Keypair,
    ) -> Result<Self> {
        let public_key = key_pair.public();

        // Create an mDNS behaviour with default configuration if the mDNS flag is set
        let mdns = if network_config.mdns {
            debug!("mDNS network behaviour enabled");
            Some(mdns::Behaviour::new(Default::default(), peer_id)?)
        } else {
            None
        };

        // Create a ping behaviour with default configuration if the ping flag is set
        let ping = if network_config.ping {
            debug!("Ping network behaviour enabled");
            Some(ping::Behaviour::default())
        } else {
            None
        };

        // Create a rendezvous client behaviour with default configuration if the rendezvous client
        // flag is set
        let rendezvous_client = if network_config.rendezvous_client {
            debug!("Rendezvous client network behaviour enabled");
            Some(rendezvous::client::Behaviour::new(key_pair))
        } else {
            None
        };

        // Create a rendezvous server behaviour with default configuration if the rendezvous server
        // flag is set
        let rendezvous_server = if network_config.rendezvous_server {
            debug!("Rendezvous server network behaviour enabled");
            Some(rendezvous::server::Behaviour::new(
                rendezvous::server::Config::default(),
            ))
        } else {
            None
        };

        // Create an identify server behaviour with default configuration if either the rendezvous
        // client or server flag is set
        let identify = if network_config.rendezvous_client || network_config.rendezvous_server {
            debug!("Identify network behaviour enabled");
            Some(identify::Behaviour::new(identify::Config::new(
                format!("{NODE_NAMESPACE}/1.0.0"),
                public_key,
            )))
        } else {
            None
        };

        let relay_server = if network_config.relay_server {
            debug!("Relay server network behaviour enabled");
            Some(relay::Behaviour::new(peer_id, relay::Config::default()))
        } else {
            None
        };

        Ok(Self {
            mdns: mdns.into(), // Convert the `Option` into a `Toggle`
            ping: ping.into(),
            rendezvous_client: rendezvous_client.into(),
            rendezvous_server: rendezvous_server.into(),
            identify: identify.into(),
            relay_server: relay_server.into(),
        })
    }
}
