// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::quic;
use libp2p::{identity, PeerId, Transport};
use log::{info, warn};
use tokio::task;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};

pub async fn libp2p_service(
    _context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let mut _rx = tx.subscribe();

    // Create a random PeerId
    let keypair = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(keypair.public());
    info!("Local peer id: {peer_id:?}");

    // Create a quic transport.
    let quic_config = quic::Config::new(&keypair);
    let mut quic_transport = quic::tokio::Transport::new(quic_config)
        // Not sure why we need to do this convertion to a StreamMuxerBox here, but I found that
        // it's necessary. Maybe it's because quic handles multiplexing for us already.
        .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
        .boxed();

    // The address we will listen on.
    let addr = "/ip4/127.0.0.1/udp/12345/quic-v1"
        .parse()
        .expect("address should be valid");

    // Start listening.
    quic_transport.listen_on(addr).expect("listen error.");

    // In a seperate thread we will log stream events.
    let handle = task::spawn(async move {
        loop {
            match quic_transport.next().await.unwrap() {
                libp2p::core::transport::TransportEvent::NewAddress {
                    listener_id,
                    listen_addr,
                } => info!("NewAddress: {0:?} {1:?}", listener_id, listen_addr),
                libp2p::core::transport::TransportEvent::AddressExpired {
                    listener_id,
                    listen_addr,
                } => info!("AddressExpired: {0:?} {1:?}", listener_id, listen_addr),
                libp2p::core::transport::TransportEvent::Incoming {
                    listener_id,
                    upgrade,
                    local_addr,
                    send_back_addr,
                } => info!(
                    "Incoming: {0:?} {1:?} {2:?}",
                    listener_id, local_addr, send_back_addr
                ),
                libp2p::core::transport::TransportEvent::ListenerClosed {
                    listener_id,
                    reason,
                } => info!("ListenerClosed: {0:?} {1:?}", listener_id, reason),
                libp2p::core::transport::TransportEvent::ListenerError { listener_id, error } => {
                    info!("ListenerError: {0:?} {1:?}", listener_id, error)
                }
            }
        }
    });

    info!("libp2p service is ready");
    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about libp2p service being ready");
    };

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = shutdown => {
        },
    }

    Ok(())
}
