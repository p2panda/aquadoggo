// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use libp2p::identity::Keypair;
use libp2p::swarm::SwarmBuilder;
use libp2p::PeerId;
use libp2p::Swarm;
use log::info;

use crate::network::behaviour::Behaviour;
use crate::network::transport;
use crate::network::NetworkConfiguration;

pub async fn build_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<(Swarm<Behaviour>, PeerId)> {
    // Read the peer ID (public key) from the key pair
    let peer_id = PeerId::from(key_pair.public());
    info!("Network service peer ID: {peer_id}");

    let relay_client_enabled = network_config.relay_address.is_some();

    let (transport, relay_client) =
        transport::build_transport(&key_pair, relay_client_enabled).await;

    // Instantiate the custom network behaviour with default configuration
    // and the libp2p peer ID
    let behaviour = Behaviour::new(network_config, peer_id, key_pair, relay_client)?;

    // Initialise a swarm with QUIC transports, our composed network behaviour
    // and the default configuration parameters
    let swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id)
        // This method expects a NonZeroU8 as input, hence the try_into conversion
        .dial_concurrency_factor(network_config.dial_concurrency_factor.try_into()?)
        .per_connection_event_buffer_size(network_config.per_connection_event_buffer_size)
        .notify_handler_buffer_size(network_config.notify_handler_buffer_size.try_into()?)
        .build();

    Ok((swarm, peer_id))
}
