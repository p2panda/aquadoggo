// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::Duration;

use futures::future::Either;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::identity::Keypair;
use libp2p::noise::Config as NoiseConfig;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::{relay, tcp, PeerId, Transport};
use libp2p_quic as quic;

// Build the transport stack to be used by the network swarm
#[allow(deprecated)]
pub async fn build_client_transport(
    key_pair: &Keypair,
) -> (Boxed<(PeerId, StreamMuxerBox)>, relay::client::Behaviour) {
    // Create QUIC transport (provides transport, security and multiplexing in a single protocol).
    // The QUIC transport will also be used for any relayed connections
    let quic_config = quic::Config::new(key_pair);
    let quic_transport = quic::tokio::Transport::new(quic_config);

    // Generate a relay transport and client behaviour if relay client mode is enabled
    let (relay_transport, relay_client) = relay::client::new(key_pair.public().to_peer_id());

    // Add encryption and multiplexing to the relay transport
    let relay_transport = relay_transport
        .upgrade(Version::V1)
        .authenticate(NoiseConfig::new(key_pair).unwrap())
        .multiplex(YamuxConfig::default())
        .timeout(Duration::from_secs(20));

    // The relay transport only handles listening and dialing on a relayed Multiaddr; it depends
    // on another transport to do the actual transmission of data. The relay transport is combined
    // with the QUIC transport by calling `OrTransport`.
    let transport = OrTransport::new(quic_transport, relay_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    (transport, relay_client)
}

// Build the transport stack to be used by the network swarm
pub async fn build_relay_transport(key_pair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let tcp_transport =
        tcp::async_io::Transport::new(tcp::Config::new().port_reuse(true).nodelay(true))
            .upgrade(Version::V1)
            .authenticate(NoiseConfig::new(&key_pair).unwrap())
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20));

    let quic_transport = {
        let mut config = libp2p_quic::Config::new(&key_pair);
        config.support_draft_29 = true;
        libp2p_quic::tokio::Transport::new(config)
    };

    OrTransport::new(quic_transport, tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed()
}
