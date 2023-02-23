// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::ping::Event;
use libp2p::swarm::{keep_alive, ConnectionLimits, NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{identity, PeerId, Transport};
use libp2p::{ping, quic, Multiaddr};
use log::{info, warn};
use serde::{Deserialize, Serialize};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};

/// Libp2p config for the node.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Libp2pConfig {
    /// Local address.
    pub listening_multiaddr: Multiaddr,

    /// Mdns discovery enabled.
    pub mdns: bool,

    /// Maximum outgoing connections.
    pub max_connections_out: u32,

    /// Maximum incoming connections.
    pub max_connections_in: u32,

    /// Maximum pending outgoing connections.
    pub max_connections_pending_out: u32,

    /// Maximum pending incoming connections.
    pub max_connections_pending_in: u32,

    /// Maximum connections per peer (includes outgoing and incoming).
    pub max_connections_per_peer: u32,

    /// Notify handler buffer size.
    ///
    /// Defines the buffer size for events sent from the NetworkBehaviour to the ConnectionHandler.
    /// If the buffer is exceeded, the Swarm will have to wait. An individual buffer with this
    /// number of events exists for each individual connection.
    pub notify_handler_buffer_size: usize,

    /// Connection event buffer size.
    ///
    /// Defines the additional buffer size for events sent from the ConnectionHandler to the
    /// NetworkBehaviour. A shared buffer exists with one available "slot" per connection. This
    /// value defines the number of additional slots per connection. If the buffer is exceeded,
    /// the ConnectionHandler will sleep.
    pub connection_event_buffer_size: usize,

    /// Dial concurrency factor.
    ///
    /// Number of addresses concurrently dialed for a single outbound
    /// connection attempt.
    pub dial_concurrency_factor: u8,
}

impl Default for Libp2pConfig {
    fn default() -> Self {
        Self {
            // Listen on 127.0.0.1 and a random, OS-assigned port
            listening_multiaddr: "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
            mdns: false,
            max_connections_pending_out: 8,
            max_connections_pending_in: 8,
            max_connections_in: 16,
            max_connections_out: 16,
            max_connections_per_peer: 8,
            notify_handler_buffer_size: 128,
            connection_event_buffer_size: 128,
            dial_concurrency_factor: 8,
        }
    }
}

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
    let libp2p_config = Libp2pConfig::default();

    // Define the connection limits of the swarm
    let connection_limits = ConnectionLimits::default()
        .with_max_pending_outgoing(Some(libp2p_config.max_connections_pending_out))
        .with_max_pending_incoming(Some(libp2p_config.max_connections_pending_in))
        .with_max_established_outgoing(Some(libp2p_config.max_connections_out))
        .with_max_established_incoming(Some(libp2p_config.max_connections_in))
        .with_max_established_per_peer(Some(libp2p_config.max_connections_per_peer));

    // Initialise a swarm with QUIC transports, our behaviour (`keep_alive` and `ping`) defined below
    // and the default configuration parameters
    let mut swarm =
        SwarmBuilder::with_tokio_executor(quic_transport, Behaviour::default(), peer_id)
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
                SwarmEvent::BannedPeer {
                    peer_id,
                    endpoint: _,
                } => info!("BannedPeer: {peer_id:?}"),
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
                SwarmEvent::Behaviour(BehaviourEvent::Ping(Event { peer, result: _ })) => {
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
/// For illustrative purposes, this includes the [`KeepAlive`](behaviour::KeepAlive) behaviour so
/// a continuous sequence of pings can be observed.
#[derive(NetworkBehaviour, Default)]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    ping: ping::Behaviour,
}
