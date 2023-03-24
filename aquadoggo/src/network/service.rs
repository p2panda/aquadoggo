// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use futures::future::Either;
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::identity::Keypair;
use libp2p::multiaddr::Protocol;
use libp2p::noise::NoiseAuthenticated;
use libp2p::ping::Event;
use libp2p::swarm::{AddressScore, SwarmBuilder, SwarmEvent};
use libp2p::yamux::YamuxConfig;
use libp2p::Swarm;
use libp2p::{autonat, identify, mdns, quic, relay, rendezvous, Multiaddr, PeerId, Transport};
use log::{debug, info, warn};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::{Behaviour, BehaviourEvent};
use crate::network::config::NODE_NAMESPACE;
use crate::network::NetworkConfiguration;

// Build the transport stack to be used by the network swarm
async fn build_transport(
    key_pair: &Keypair,
) -> (
    Boxed<(PeerId, StreamMuxerBox)>,
    Option<relay::client::Behaviour>,
) {
    // Generate a relay transport and client behaviour if relay client mode is selected
    let (relay_transport, relay_client) = relay::client::new(key_pair.public().to_peer_id());
    let relay_client = Some(relay_client);

    // Add encryption and multiplexing to the relay transport
    let relay_transport = relay_transport
        .upgrade(Version::V1)
        .authenticate(NoiseAuthenticated::xx(key_pair).unwrap())
        .multiplex(YamuxConfig::default());

    // Create QUIC transport (provides transport, security and multiplexing in a single protocol)
    let quic_config = quic::Config::new(key_pair);
    let quic_transport = quic::tokio::Transport::new(quic_config);

    let transport = OrTransport::new(quic_transport, relay_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    (transport, relay_client)
}

async fn build_swarm(
    network_config: &NetworkConfiguration,
    key_pair: Keypair,
) -> Result<Swarm<Behaviour>> {
    // Read the peer ID (public key) from the key pair
    let peer_id = PeerId::from(key_pair.public());
    info!("Network service peer ID: {peer_id}");

    let (transport, relay_client) = build_transport(&key_pair).await;

    // Instantiate the custom network behaviour with default configuration
    // and the libp2p peer ID
    let behaviour = Behaviour::new(network_config, peer_id, key_pair, relay_client)?;

    // Initialise a swarm with QUIC transports, our composed network behaviour
    // and the default configuration parameters
    let swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id)
        .connection_limits(network_config.connection_limits())
        // This method expects a NonZeroU8 as input, hence the try_into conversion
        .dial_concurrency_factor(network_config.dial_concurrency_factor.try_into()?)
        .per_connection_event_buffer_size(network_config.per_connection_event_buffer_size)
        .notify_handler_buffer_size(network_config.notify_handler_buffer_size.try_into()?)
        .build();

    Ok(swarm)
}

/// Network service that configures and deploys a network swarm over QUIC transports.
///
/// The swarm listens for incoming connections, dials remote nodes, manages
/// connections and executes predefined network behaviours.
pub async fn network_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let mut _rx = tx.subscribe();

    // Load the network key pair and peer ID
    let key_pair =
        NetworkConfiguration::load_or_generate_key_pair(context.config.base_path.clone())?;

    // Read the network configuration parameters from the application context
    let network_config = context.config.network.clone();

    // Build the network swarm
    let mut swarm = build_swarm(&network_config, key_pair).await?;

    // Define the QUIC multiaddress on which the swarm will listen for connections
    let quic_multiaddr =
        format!("/ip4/0.0.0.0/udp/{}/quic-v1", network_config.quic_port).parse()?;

    // Listen for incoming connection requests over the QUIC transport
    swarm.listen_on(quic_multiaddr)?;

    // Dial each peer identified by the multi-address provided via `--remote-node-addresses` if given
    for addr in network_config.remote_peers.clone() {
        swarm.dial(addr)?
    }

    // Dial the peer identified by the multi-address provided via `--rendezvous_address` if given
    if let Some(addr) = network_config.rendezvous_address.clone() {
        swarm.dial(addr)?;
    }

    // Create a cookie holder for the identify service
    let mut cookie = None;

    // Create a public address holder for the local node, to be updated via AutoNAT
    let mut public_addr: Option<Multiaddr> = None;

    // Spawn a task to handle swarm events
    let handle = tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::BannedPeer {
                    peer_id,
                    endpoint: _,
                } => debug!("BannedPeer: {peer_id}"),
                SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                    mdns::Event::Discovered(list) => {
                        for (peer, _multiaddr) in list {
                            debug!("mDNS discovered a new peer: {peer}");
                        }
                    }
                    mdns::Event::Expired(list) => {
                        for (peer, _multiaddr) in list {
                            debug!("mDNS peer has expired: {peer}");
                        }
                    }
                },
                SwarmEvent::Behaviour(BehaviourEvent::Ping(Event { peer, result: _ })) => {
                    debug!("Ping from: {peer}")
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    endpoint,
                    num_established,
                    cause,
                } => {
                    debug!("ConnectionClosed: {peer_id} {endpoint:?} {num_established} {cause:?}")
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. }
                    if network_config.rendezvous_client
                        && network_config.rendezvous_peer_id.unwrap() == peer_id =>
                {
                    debug!(
                        "Connected to rendezvous point, discovering nodes in '{NODE_NAMESPACE}' namespace ..."
                    );

                    swarm
                        .behaviour_mut()
                        .rendezvous_client
                        .as_mut()
                        .unwrap()
                        .discover(
                            Some(rendezvous::Namespace::new(NODE_NAMESPACE.to_string()).unwrap()),
                            None,
                            None,
                            network_config
                                .rendezvous_peer_id
                                .expect("Rendezvous server peer ID was provided"),
                        );
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    num_established,
                    ..
                } => debug!("ConnectionEstablished: {peer_id} {endpoint:?} {num_established}"),
                SwarmEvent::Dialing(peer_id) => info!("Dialing: {peer_id}"),
                SwarmEvent::ExpiredListenAddr {
                    listener_id,
                    address,
                } => debug!("ExpiredListenAddr: {listener_id:?} {address}"),

                SwarmEvent::IncomingConnection {
                    local_addr,
                    send_back_addr,
                } => debug!("IncomingConnection: {local_addr} {send_back_addr}"),
                SwarmEvent::IncomingConnectionError {
                    local_addr,
                    send_back_addr,
                    error,
                } => warn!("IncomingConnectionError: {local_addr} {send_back_addr} {error:?}"),
                SwarmEvent::ListenerClosed {
                    listener_id,
                    addresses,
                    reason,
                } => debug!("ListenerClosed: {listener_id:?} {addresses:?} {reason:?}"),
                SwarmEvent::ListenerError { listener_id, error } => {
                    warn!("ListenerError: {listener_id:?} {error:?}")
                }
                SwarmEvent::NewListenAddr {
                    address,
                    listener_id: _,
                } => {
                    info!("Listening on {address}");
                }
                SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                    warn!("OutgoingConnectionError: {peer_id:?} {error:?}")
                }
                SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(event)) => match event {
                    rendezvous::client::Event::Registered {
                        namespace,
                        ttl,
                        rendezvous_node,
                    } => {
                        debug!("Registered for namespace '{namespace}' at rendezvous point {rendezvous_node} for the next {ttl} seconds")
                    }
                    rendezvous::client::Event::Discovered {
                        registrations,
                        cookie: new_cookie,
                        ..
                    } => {
                        debug!("Rendezvous point responded with peer registration data");

                        cookie.replace(new_cookie);

                        for registration in registrations {
                            for address in registration.record.addresses() {
                                let peer = registration.record.peer_id();
                                debug!("Discovered peer {peer} at {address}");

                                let p2p_suffix = Protocol::P2p(*peer.as_ref());
                                let address_with_p2p = if !address
                                    .ends_with(&Multiaddr::empty().with(p2p_suffix.clone()))
                                {
                                    address.clone().with(p2p_suffix)
                                } else {
                                    address.clone()
                                };

                                swarm.dial(address_with_p2p).unwrap();
                            }
                        }
                    }
                    rendezvous::client::Event::RegisterFailed(error) => {
                        warn!("Failed to register with rendezvous point: {error}");
                    }
                    other => debug!("Unhandled rendezvous client event: {other:?}"),
                },
                SwarmEvent::Behaviour(BehaviourEvent::RendezvousServer(event)) => match event {
                    rendezvous::server::Event::PeerRegistered { peer, registration } => {
                        debug!(
                            "Peer {peer} registered for namespace '{}'",
                            registration.namespace
                        );
                    }
                    rendezvous::server::Event::DiscoverServed {
                        enquirer,
                        registrations,
                    } => {
                        debug!(
                            "Served peer {enquirer} with {} registrations",
                            registrations.len()
                        );
                    }
                    other => debug!("Unhandled rendezvous server event: {other:?}"),
                },
                SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                    match event {
                        identify::Event::Received { peer_id, info } => {
                            debug!("Received identify information from peer {peer_id}");
                            debug!(
                                "Peer {peer_id} reported local external address: {}",
                                info.observed_addr
                            );

                            swarm.add_external_address(info.observed_addr, AddressScore::Infinite);

                            // Only attempt registration if the local node is running as a rendezvous client
                            if network_config.rendezvous_client {
                                // Once `identify` information is received from a remote peer, the external
                                // address of the local node is known and registration with the rendezvous
                                // server can be carried out.

                                // We call `as_mut()` on the rendezvous client network behaviour in
                                // order to get a mutable reference out of the `Toggle`
                                swarm
                                    .behaviour_mut()
                                    .rendezvous_client
                                    .as_mut()
                                    .unwrap()
                                    .register(
                                        rendezvous::Namespace::from_static(NODE_NAMESPACE),
                                        network_config
                                            .rendezvous_peer_id
                                            .expect("Rendezvous server peer ID was provided"),
                                        None,
                                    );
                            }
                        }
                        identify::Event::Sent { peer_id } | identify::Event::Pushed { peer_id } => {
                            debug!(
                                "Sent identification information of the local node to peer {peer_id}"
                            )
                        }
                        identify::Event::Error { peer_id, error } => {
                            warn!("Failed to identify the remote peer {peer_id}: {error}")
                        }
                    }
                }
                SwarmEvent::Behaviour(BehaviourEvent::RelayServer(event)) => {
                    debug!("Unhandled relay server event: {event:?}")
                }
                SwarmEvent::Behaviour(BehaviourEvent::RelayClient(_)) => todo!(),
                SwarmEvent::Behaviour(BehaviourEvent::Autonat(event)) => {
                    match event {
                        autonat::Event::StatusChanged { old, new } => match (old, new) {
                            (autonat::NatStatus::Unknown, autonat::NatStatus::Private) => {
                                if swarm.behaviour().relay_client.is_enabled() {
                                    if let Some(addr) = &network_config.relay_address {
                                        let circuit_addr = addr.clone().with(Protocol::P2pCircuit);
                                        warn!("Private NAT detected. Listening on relay circuit address");
                                        swarm
                                            .listen_on(circuit_addr)
                                            .expect("Failed to listen on relay circuit address");
                                    }
                                }
                            }
                            (_, autonat::NatStatus::Public(addr)) => {
                                info!("Public NAT verified! Public listening address: {}", addr);
                                public_addr = Some(addr);
                            }
                            (old, new) => {
                                warn!("NAT status changed from {:?} to {:?}", old, new);
                            }
                        },
                        autonat::Event::InboundProbe(_) | autonat::Event::OutboundProbe(_) => (),
                    }
                }
            }
        }
    });

    info!("Network service is ready");

    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about network service being ready");
    };

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    Ok(())
}
