// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::{noise, tcp, yamux, Swarm, SwarmBuilder};

use crate::network::behaviour::P2pandaBehaviour;
use crate::network::NetworkConfiguration;

pub async fn build_relay_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let swarm = SwarmBuilder::with_existing_identity(key_pair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().port_reuse(true).nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key_pair| P2pandaBehaviour::new(network_config, key_pair, None).unwrap())?
        .build();
    Ok(swarm)
}

pub async fn build_client_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let swarm = SwarmBuilder::with_existing_identity(key_pair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key_pair, relay_client| {
            P2pandaBehaviour::new(network_config, key_pair, Some(relay_client)).unwrap()
        })?
        .build();
    Ok(swarm)
}
