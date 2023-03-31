// SPDX-License-Identifier: AGPL-3.0-or-later

use futures::future::Either;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::identity::Keypair;
use libp2p::noise::NoiseAuthenticated;
use libp2p::yamux::YamuxConfig;
use libp2p::{quic, relay, PeerId, Transport};

// Build the transport stack to be used by the network swarm
pub async fn build_transport(
    key_pair: &Keypair,
    relay_client_enabled: bool,
) -> (
    Boxed<(PeerId, StreamMuxerBox)>,
    Option<relay::client::Behaviour>,
) {
    // Create QUIC transport (provides transport, security and multiplexing in a single protocol)
    let quic_config = quic::Config::new(key_pair);
    let quic_transport = quic::tokio::Transport::new(quic_config);

    let (transport, relay_client) = if relay_client_enabled {
        // Generate a relay transport and client behaviour if relay client mode is enabled
        let (relay_transport, relay_client) = relay::client::new(key_pair.public().to_peer_id());
        let relay_client = Some(relay_client);

        // Add encryption and multiplexing to the relay transport
        let relay_transport = relay_transport
            .upgrade(Version::V1)
            .authenticate(NoiseAuthenticated::xx(key_pair).unwrap())
            .multiplex(YamuxConfig::default());

        // The appropriate transport will be matched and utilised for connections (ie.
        // relayed-connections will utilise the relay transport while all other incoming
        // and outgoing connections will utilise QUIC)
        let transport = OrTransport::new(quic_transport, relay_transport)
            .map(|either_output, _| match either_output {
                Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            })
            .boxed();

        (transport, relay_client)
    } else {
        let transport = quic_transport
            // Perform conversion to a StreamMuxerBox (QUIC handles multiplexing)
            .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
            .boxed();

        (transport, None)
    };

    (transport, relay_client)
}
