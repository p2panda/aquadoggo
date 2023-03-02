// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::ping::Event;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmBuilder, SwarmEvent};
use libp2p::{mdns, ping, quic, Multiaddr, PeerId, Transport};
use log::{info, warn};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::libp2p::Libp2pConfiguration;
use crate::manager::{ServiceReadySender, Shutdown};

/// Network behaviour for the aquadoggo node.
#[derive(NetworkBehaviour)]
struct Behaviour {
    /// Automatically discover peers on the local network.
    mdns: Toggle<mdns::tokio::Behaviour>,
    /// Respond to inbound pings and periodically send outbound ping on every established
    /// connection.
    ping: Toggle<ping::Behaviour>,
}

impl Behaviour {
    /// Generate a new instance of the composed network behaviour according to
    /// the libp2p configuration context.
    fn new(libp2p_config: &Libp2pConfiguration, peer_id: PeerId) -> Result<Self> {
        // Create an mDNS behaviour with default configuration if the mDNS flag is set
        let mdns = if libp2p_config.mdns {
            Some(mdns::Behaviour::new(Default::default(), peer_id)?)
        } else {
            None
        };

        // Create a ping behaviour with default configuration if the ping flag is set
        let ping = if libp2p_config.ping {
            Some(ping::Behaviour::default())
        } else {
            None
        };

        Ok(Self {
            mdns: mdns.into(), // Convert the `Option` into a `Toggle`
            ping: ping.into(),
        })
    }
}

/// Libp2p service that configures and deploys a network swarm over QUIC transports.
///
/// The swarm listens for incoming connections, dials remote nodes, manages
/// connections and executes predefined network behaviours.
pub async fn libp2p_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let mut _rx = tx.subscribe();

    // Read the libp2p configuration parameters from the application context
    let libp2p_config = context.config.libp2p.clone();

    // Load the libp2p keypair and peer ID
    let keypair = Libp2pConfiguration::load_or_generate_keypair(context.config.base_path.clone())?;
    let peer_id = PeerId::from(keypair.public());
    info!("libp2p peer ID: {peer_id:?}");

    // Create a QUIC transport
    let quic_config = quic::Config::new(&keypair);
    // QUIC provides transport, security, and multiplexing in a single protocol
    let quic_transport = quic::tokio::Transport::new(quic_config)
        // Perform conversion to a StreamMuxerBox (QUIC handles multiplexing)
        .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
        .boxed();

    // Instantiate the custom network behaviour with default configuration
    // and the libp2p peer ID
    let behaviour = Behaviour::new(&libp2p_config, peer_id)?;

    // Initialise a swarm with QUIC transports, our composed network behaviour
    // and the default configuration parameters
    let mut swarm = SwarmBuilder::with_tokio_executor(quic_transport, behaviour, peer_id)
        .connection_limits(libp2p_config.connection_limits())
        // This method expects a NonZeroU8 as input, hence the try_into conversion
        .dial_concurrency_factor(libp2p_config.dial_concurrency_factor.try_into()?)
        .per_connection_event_buffer_size(libp2p_config.per_connection_event_buffer_size)
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
