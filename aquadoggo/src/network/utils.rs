// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;

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
