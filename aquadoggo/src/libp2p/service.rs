// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::ping::Event;
use libp2p::swarm::{keep_alive, NetworkBehaviour, SwarmEvent};
use libp2p::{identity, PeerId, Swarm, Transport};
use libp2p::{ping, quic, Multiaddr};
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
/// `RUST_LOG=info cargo run`
///
/// Then run as a dialer which attempts to dial the above listener, taking the correct multiaddr
/// from the logging in the previous peer:
///
/// `RUST_LOG=info cargo run -- --http-port 2021 --remote-node-addresses "/ip4/127.0.0.1/udp/<PORT>/quic-v1"`
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
    let peer_id = PeerId::from(keypair.public());
    info!("Peer id: {peer_id:?}");

    // Create a quic transport.
    let quic_config = quic::Config::new(&keypair);
    let quic_transport = quic::tokio::Transport::new(quic_config)
        // Not sure why we need to do this conversion to a StreamMuxerBox here, but I found that
        // it's necessary. Maybe it's because quic handles multiplexing for us already.
        .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
        .boxed();

    // Init swarm with our behaviour (`keep_alive` and `ping`) defined below
    let mut swarm = Swarm::with_tokio_executor(quic_transport, Behaviour::default(), peer_id);

    // Tell the swarm to listen on 127.0.0.1 and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;

    // Dial the peer identified by the multi-address given in the `--remote-node-addresses` if given
    if let Some(addr) = context.config.replication.remote_peers.get(0) {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        println!("Dialed {addr}")
    }

    // Spawn a task logging swarm events
    let handle = tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::NewListenAddr {
                    address,
                    listener_id,
                } => {
                    info!("Listening on {address:?} {listener_id:?}");
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    num_established,
                    ..
                } => info!("ConnectionEstablished: {peer_id:?} {endpoint:?} {num_established}"),
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    endpoint,
                    num_established,
                    cause,
                } => {
                    info!("ConnectionClosed: {peer_id:?} {endpoint:?} {num_established} {cause:?}")
                }
                SwarmEvent::IncomingConnection {
                    local_addr,
                    send_back_addr,
                } => info!("IncomingConnection: {local_addr:?} {send_back_addr:?}"),
                SwarmEvent::IncomingConnectionError {
                    local_addr,
                    send_back_addr,
                    error,
                } => info!("IncomingConnectionError: {local_addr:?} {send_back_addr:?} {error:?}"),
                SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                    info!("OutgoingConnectionError: {peer_id:?} {error:?}")
                }
                SwarmEvent::BannedPeer { peer_id, endpoint } => info!("BannedPeer: {peer_id:?}"),
                SwarmEvent::ExpiredListenAddr {
                    listener_id,
                    address,
                } => info!("ExpiredListenAddr: {listener_id:?} {address:?}"),
                SwarmEvent::ListenerClosed {
                    listener_id,
                    addresses,
                    reason,
                } => info!("ListenerClosed: {listener_id:?} {addresses:?} {reason:?}"),
                SwarmEvent::ListenerError { listener_id, error } => {
                    info!("ListenerError: {listener_id:?} {error:?}")
                }
                SwarmEvent::Dialing(peer_id) => info!("Dialing: {peer_id:?}"),
                SwarmEvent::Behaviour(BehaviourEvent::KeepAlive(_)) => info!("Keep alive"),
                SwarmEvent::Behaviour(BehaviourEvent::Ping(Event { peer, result })) => {
                    info!("Ping from: {peer:?}")
                }
            };
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

/// Our network behaviour.
///
/// For illustrative purposes, this includes the [`KeepAlive`](behaviour::KeepAlive) behaviour so a continuous sequence of
/// pings can be observed.
#[derive(NetworkBehaviour, Default)]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    ping: ping::Behaviour,
}
