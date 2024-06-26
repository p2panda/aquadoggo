// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use either::Either;
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::pnet::PnetConfig;
use libp2p::{noise, tcp, yamux, Swarm, SwarmBuilder, Transport};

use crate::network::behaviour::P2pandaBehaviour;
use crate::network::NetworkConfiguration;

pub fn build_tcp_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let swarm = SwarmBuilder::with_existing_identity(key_pair)
        .with_tokio()
        .with_other_transport(|key| {
            let noise_config = noise::Config::new(key).unwrap();
            let yamux_config = yamux::Config::default();

            let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
            let maybe_encrypted = match network_config.psk {
                Some(psk) => Either::Left(
                    base_transport
                        .and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
                ),
                None => Either::Right(base_transport),
            };
            maybe_encrypted
                .upgrade(Version::V1Lazy)
                .authenticate(noise_config)
                .multiplex(yamux_config)
        })?;

    let swarm = if !network_config.relay_mode && !network_config.relay_addresses.is_empty() {
        swarm
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|key_pair, relay_client| {
                P2pandaBehaviour::new(network_config, key_pair, Some(relay_client)).unwrap()
            })?
            .build()
    } else {
        swarm
            .with_behaviour(|key_pair| {
                P2pandaBehaviour::new(network_config, key_pair, None).unwrap()
            })?
            .build()
    };

    Ok(swarm)
}

pub fn build_quic_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let swarm = SwarmBuilder::with_existing_identity(key_pair)
        .with_tokio()
        .with_quic();

    let swarm = if !network_config.relay_mode && !network_config.relay_addresses.is_empty() {
        swarm
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|key_pair, relay_client| {
                P2pandaBehaviour::new(network_config, key_pair, Some(relay_client)).unwrap()
            })?
            .build()
    } else {
        swarm
            .with_behaviour(|key_pair| {
                P2pandaBehaviour::new(network_config, key_pair, None).unwrap()
            })?
            .build()
    };

    Ok(swarm)
}
