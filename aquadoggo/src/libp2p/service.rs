// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::ping::Event;
use libp2p::swarm::{keep_alive, ConnectionLimits, NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{identity, mdns, ping, quic, Multiaddr, PeerId, Transport};
use log::{info, warn};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::libp2p::Libp2pConfiguration;
use crate::manager::{ServiceReadySender, Shutdown};

/// Libp2p service that configures and deploys a swarm over QUIC transports.
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

    // Create a QUIC transport
    let quic_config = quic::Config::new(&keypair);
    // QUIC provides transport, security, and multiplexing in a single protocol
    let quic_transport = quic::tokio::Transport::new(quic_config)
        // Not sure why we need to do this conversion to a StreamMuxerBox here, but I found that
        // it's necessary. Maybe it's because quic handles multiplexing for us already.
        .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
        .boxed();

    // Define the swarm configuration parameters
    let libp2p_config = Libp2pConfiguration::default();

    // Define the connection limits of the swarm
    let connection_limits = ConnectionLimits::default()
        .with_max_pending_outgoing(Some(libp2p_config.max_connections_pending_out))
        .with_max_pending_incoming(Some(libp2p_config.max_connections_pending_in))
        .with_max_established_outgoing(Some(libp2p_config.max_connections_out))
        .with_max_established_incoming(Some(libp2p_config.max_connections_in))
        .with_max_established_per_peer(Some(libp2p_config.max_connections_per_peer));

    // Create an mDNS behaviour with default configuration
    let mdns_behaviour = mdns::Behaviour::new(Default::default())?;

    // Instantiate the custom network behaviour with defaults
    let behaviour = Behaviour {
        keep_alive: keep_alive::Behaviour::default(),
        ping: ping::Behaviour::default(),
        mdns: mdns_behaviour,
    };

    // Initialise a swarm with QUIC transports, our behaviour (`keep_alive` and `ping`) defined below
    // and the default configuration parameters
    let mut swarm = SwarmBuilder::with_tokio_executor(quic_transport, behaviour, peer_id)
        .connection_limits(connection_limits)
        // This method expects a NonZeroU8 as input, hence the try_into conversion
        .dial_concurrency_factor(libp2p_config.dial_concurrency_factor.try_into()?)
        .connection_event_buffer_size(libp2p_config.connection_event_buffer_size)
        .notify_handler_buffer_size(libp2p_config.notify_handler_buffer_size.try_into()?)
        .build();

    // Tell the swarm to listen on the default multiaddress
    swarm.listen_on(libp2p_config.listening_multiaddr)?;

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
                SwarmEvent::BannedPeer {
                    peer_id,
                    endpoint: _,
                } => info!("BannedPeer: {peer_id:?}"),
                SwarmEvent::Behaviour(BehaviourEvent::KeepAlive(_)) => info!("Keep alive"),
                SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                    mdns::Event::Discovered(list) => {
                        for (peer, _multiaddr) in list {
                            info!("mDNS discovered a new peer: {peer:?}");
                        }
                    }
                    mdns::Event::Expired(list) => {
                        for (peer, _multiaddr) in list {
                            info!("mDNS peer has expired: {peer:?}");
                        }
                    }
                },
                SwarmEvent::Behaviour(BehaviourEvent::Ping(Event { peer, result: _ })) => {
                    info!("Ping from: {peer:?}")
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    endpoint,
                    num_established,
                    cause,
                } => {
                    info!("ConnectionClosed: {peer_id:?} {endpoint:?} {num_established} {cause:?}")
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    num_established,
                    ..
                } => info!("ConnectionEstablished: {peer_id:?} {endpoint:?} {num_established}"),

                SwarmEvent::Dialing(peer_id) => info!("Dialing: {peer_id:?}"),
                SwarmEvent::ExpiredListenAddr {
                    listener_id,
                    address,
                } => info!("ExpiredListenAddr: {listener_id:?} {address:?}"),

                SwarmEvent::IncomingConnection {
                    local_addr,
                    send_back_addr,
                } => info!("IncomingConnection: {local_addr:?} {send_back_addr:?}"),
                SwarmEvent::IncomingConnectionError {
                    local_addr,
                    send_back_addr,
                    error,
                } => info!("IncomingConnectionError: {local_addr:?} {send_back_addr:?} {error:?}"),
                SwarmEvent::ListenerClosed {
                    listener_id,
                    addresses,
                    reason,
                } => info!("ListenerClosed: {listener_id:?} {addresses:?} {reason:?}"),
                SwarmEvent::ListenerError { listener_id, error } => {
                    info!("ListenerError: {listener_id:?} {error:?}")
                }
                SwarmEvent::NewListenAddr {
                    address,
                    listener_id,
                } => {
                    info!("Listening on {address:?} {listener_id:?}");
                }
                SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                    info!("OutgoingConnectionError: {peer_id:?} {error:?}")
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
/// For illustrative purposes, this includes the [`KeepAlive`](behaviour::KeepAlive) behaviour so
/// a continuous sequence of pings can be observed.
#[derive(NetworkBehaviour)]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    ping: ping::Behaviour,
    mdns: mdns::tokio::Behaviour,
}
