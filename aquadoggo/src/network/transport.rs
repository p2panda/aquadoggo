// SPDX-License-Identifier: AGPL-3.0-or-later

use futures::future::Either;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::identity::Keypair;
use libp2p::noise::Config as NoiseConfig;
use libp2p::quic::tokio::Transport as QuicTransport;
use libp2p::quic::Config as QuicConfig;
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::tcp::Config as TcpConfig;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::{relay, PeerId, Transport};

fn quic_config(key_pair: &Keypair) -> QuicConfig {
    let mut config = QuicConfig::new(key_pair);
    config.support_draft_29 = true;
    config
}

// Build the transport stack to be used by nodes _not_ behaving as relays.
pub async fn build_client_transport(
    key_pair: &Keypair,
) -> (Boxed<(PeerId, StreamMuxerBox)>, relay::client::Behaviour) {
    let (relay_transport, relay_client) = relay::client::new(key_pair.public().to_peer_id());

    let transport = {
        let quic_transport = QuicTransport::new(quic_config(key_pair));

        let relay_tcp_quic_transport = relay_transport
            .or_transport(TcpTransport::new(TcpConfig::default().port_reuse(true)))
            .upgrade(Version::V1)
            .authenticate(NoiseConfig::new(key_pair).unwrap())
            .multiplex(YamuxConfig::default())
            .or_transport(quic_transport);

        relay_tcp_quic_transport
            .map(|either_output, _| match either_output {
                Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            })
            .boxed()
    };

    (transport, relay_client)
}

// Build the transport stack to be used by nodes with relay capabilities.
pub async fn build_relay_transport(key_pair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let tcp_transport = TcpTransport::new(TcpConfig::new().port_reuse(true))
        .upgrade(Version::V1)
        .authenticate(NoiseConfig::new(key_pair).unwrap())
        .multiplex(YamuxConfig::default());

    let quic_transport = QuicTransport::new(quic_config(key_pair));

    OrTransport::new(quic_transport, tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed()
}
