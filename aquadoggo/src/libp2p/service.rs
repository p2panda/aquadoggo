// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::{identity, PeerId, Transport};
use libp2p::{quic, Multiaddr};
use log::{info, warn};
use tokio::task::{self, JoinHandle};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};

/// Testing out using quic transport from libp2p.
/// 
/// This service can run as either a listener or a dialer.
/// 
/// First run as a listener like so:
/// 
/// `RUST_LOG=debug cargo run`
/// 
/// Then run as a dialer which attempts to dial the above listener:
/// 
/// `RUST_LOG=debug cargo run -- --http-port 2021 --remote-node-addresses "/ip4/127.0.0.1/udp/12345/quic-v1"`
/// 
/// Actually, nothing happens right now.... not sure why yet ;-p
pub async fn libp2p_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let mut _rx = tx.subscribe();

    // Create a random PeerId
    let keypair = identity::Keypair::generate_ed25519();
    info!("Peer id: {:?}", PeerId::from(keypair.public()));

    // Init quic transport
    let quic_transport = create_quic_transport(keypair);

    // If a remote peer multiaddress is given using the `--remote-node-addresses` flag then dial
    // it otherwise start listening for connections.  
    let handle = match context.config.replication.remote_peers.get(0) {
        Some(peer) => {
            let addr: Multiaddr = peer.parse().expect("address should be valid");
            dial(&addr, quic_transport)
        }
        None => {
            let addr: Multiaddr = "/ip4/127.0.0.1/udp/12345/quic-v1"
                .parse()
                .expect("address should be valid");
            listen(&addr, quic_transport)
        }
    };

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

fn listen(addr: &Multiaddr, mut transport: Boxed<(PeerId, StreamMuxerBox)>) -> JoinHandle<()> {
    // Start listening.
    transport.listen_on(addr.clone()).expect("listen error.");
    info!("Listening for connections....");

    // In a seperate thread we will log stream events.
    task::spawn(async move {
        loop {
            match transport.next().await.unwrap() {
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
    })
}

fn dial(addr: &Multiaddr, mut transport: Boxed<(PeerId, StreamMuxerBox)>) -> JoinHandle<()> {
    let addr = addr.clone();
    task::spawn(async move {
        info!("Dialing listener...");
        // Dial the listener from a new quic transport.
        let (peer_id, _connection) = transport
            .dial(addr)
            .expect("Can dial peer")
            .await
            .expect("Connection ok");
        info!("Connected to: {peer_id:?}");
    })
}

fn create_quic_transport(keypair: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    // Create a quic transport.
    let quic_config = quic::Config::new(&keypair);
    quic::tokio::Transport::new(quic_config)
        // Not sure why we need to do this conversion to a StreamMuxerBox here, but I found that
        // it's necessary. Maybe it's because quic handles multiplexing for us already.
        .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
        .boxed()
}
