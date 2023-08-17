// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::identity::Keypair;
use libp2p::swarm::{Swarm, SwarmBuilder};
use libp2p::PeerId;

use crate::network::behaviour::P2pandaBehaviour;
use crate::network::transport;
use crate::network::NetworkConfiguration;

fn build_swarm(
    network_config: &NetworkConfiguration,
    transport: Boxed<(PeerId, StreamMuxerBox)>,
    behaviour: P2pandaBehaviour,
    peer_id: PeerId,
) -> Result<Swarm<P2pandaBehaviour>> {
    // Initialise a swarm with QUIC transports and our composed network behaviour
    let swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id)
        // This method expects a NonZeroU8 as input, hence the try_into conversion
        .dial_concurrency_factor(network_config.dial_concurrency_factor.try_into()?)
        .per_connection_event_buffer_size(network_config.per_connection_event_buffer_size)
        .notify_handler_buffer_size(network_config.notify_handler_buffer_size.try_into()?)
        .build();

    Ok(swarm)
}

pub async fn build_relay_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let peer_id = key_pair.public().to_peer_id();
    let transport = transport::build_relay_transport(&key_pair).await;
    let behaviour = P2pandaBehaviour::new(network_config, key_pair, None)?;

    build_swarm(network_config, transport, behaviour, peer_id)
}

pub async fn build_client_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let peer_id = key_pair.public().to_peer_id();
    let (transport, relay_client) = transport::build_client_transport(&key_pair).await;
    let behaviour = P2pandaBehaviour::new(network_config, key_pair, Some(relay_client))?;

    build_swarm(network_config, transport, behaviour, peer_id)
}
