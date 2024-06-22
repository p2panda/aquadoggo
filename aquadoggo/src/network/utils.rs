// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU8;

use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{Multiaddr, PeerId, Swarm};
use log::debug;
use regex::Regex;

use crate::network::behaviour::P2pandaBehaviour;
use crate::network::config::PeerAddress;

pub fn to_quic_address(address: &Multiaddr) -> Option<SocketAddr> {
    let hay = address.to_string();
    let regex = Regex::new(r"/ip4/(\d+.\d+.\d+.\d+)/udp/(\d+)/quic-v1").unwrap();
    let caps = regex.captures(&hay);

    match caps {
        None => None,
        Some(caps) => {
            let ip_address = caps.get(1).unwrap().as_str();
            let port = caps.get(2).unwrap().as_str();
            let socket = format!("{ip_address}:{port}")
                .parse::<SocketAddr>()
                .expect("Tried to convert invalid address");
            Some(socket)
        }
    }
}

pub fn is_known_peer_address(
    known_addresses: &mut Vec<PeerAddress>,
    peer_addresses: &Vec<Multiaddr>,
) -> Option<Multiaddr> {
    for address in known_addresses.iter_mut() {
        if let Ok(addr) = address.quic_multiaddr() {
            if peer_addresses.contains(&addr) {
                return Some(addr.clone());
            }
        }
    }
    None
}

pub fn dial_known_peer(
    swarm: &mut Swarm<P2pandaBehaviour>,
    known_peers: &mut HashMap<Multiaddr, PeerId>,
    address: &mut PeerAddress,
) {
    // Get the peers quic multiaddress, this can error if the address was provided in the form
    // of a domain name and we are not able to resolve it to a valid multiaddress (for example,
    // if we are offline).
    let address = match address.quic_multiaddr() {
        Ok(address) => address,
        Err(e) => {
            debug!("Failed to resolve relay multiaddr: {}", e.to_string());
            return;
        }
    };

    // Construct dial opts depending on if we know the peer id of the peer we are dialing.
    // We know the peer id if we have connected once to the peer in the current session.
    let opts = match known_peers.get(&address) {
        Some(peer_id) => DialOpts::peer_id(*peer_id)
            .addresses(vec![address.to_owned()])
            .override_dial_concurrency_factor(NonZeroU8::new(1).expect("Is nonzero u8"))
            .build(),
        None => DialOpts::unknown_peer_id()
            .address(address.to_owned())
            .build(),
    };

    // Dial the known peer. When dialing a peer by it's peer id this method will attempt a
    // new connections if we are already connected to the peer or we are already dialing
    // them.
    match swarm.dial(opts) {
        Ok(_) => (),
        Err(err) => debug!("Error dialing node: {:?}", err),
    };
}
