// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, SocketAddr};

use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use regex::Regex;

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

pub fn to_multiaddress(socket_address: &SocketAddr) -> Multiaddr {
    let mut multiaddr = match socket_address.ip() {
        IpAddr::V4(ip) => Multiaddr::from(Protocol::Ip4(ip)),
        IpAddr::V6(ip) => Multiaddr::from(Protocol::Ip6(ip)),
    };
    multiaddr.push(Protocol::Udp(socket_address.port()));
    multiaddr.push(Protocol::QuicV1);
    multiaddr
}
