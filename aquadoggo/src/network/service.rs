// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures::StreamExt;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Event;
use libp2p::swarm::{AddressScore, SwarmEvent};
use libp2p::{autonat, identify, mdns, rendezvous, Multiaddr};
use log::{debug, info, trace, warn};

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::BehaviourEvent;
use crate::network::config::NODE_NAMESPACE;
use crate::network::swarm;
use crate::network::NetworkConfiguration;

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

    // Build the network swarm and retrieve the local peer ID
    let (mut swarm, local_peer_id) = swarm::build_swarm(&network_config, key_pair).await?;

    // Define the QUIC multiaddress on which the swarm will listen for connections
    let quic_multiaddr =
        format!("/ip4/0.0.0.0/udp/{}/quic-v1", network_config.quic_port).parse()?;

    // Listen for incoming connection requests over the QUIC transport
    swarm.listen_on(quic_multiaddr)?;

    let mut external_circuit_addr = None;

    // Construct circuit relay addresses and listen on relayed address
    if let Some(relay_addr) = &network_config.relay_address {
        if let Some(rendezvous_peer_id) = network_config.rendezvous_peer_id {
            let circuit_addr = relay_addr
                .clone()
                .with(Protocol::P2p(rendezvous_peer_id.into()))
                .with(Protocol::P2pCircuit);

            // Dialable circuit relay address for local node
            external_circuit_addr = Some(
                circuit_addr
                    .clone()
                    .with(Protocol::P2p(local_peer_id.into())),
            );

            swarm.listen_on(circuit_addr)?;
        } else {
            warn!("Unable to construct relay circuit address because rendezvous server peer ID was not provided");
        }
    }

    // Dial each peer identified by the multi-address provided via `--remote-peers` if given
    for addr in network_config.remote_peers.clone() {
        swarm.dial(addr)?
    }

    // Dial the peer identified by the multi-address provided via `--rendezvous_address` if given
    if let Some(addr) = network_config.rendezvous_address.clone() {
        swarm.dial(addr)?;
    }

    // Create a cookie holder for the identify service
    let mut cookie = None;

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
                            trace!("mDNS peer has expired: {peer}");
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
                    info!("ConnectionClosed: {peer_id} {endpoint:?} {num_established} {cause:?}")
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    num_established,
                    ..
                } => {
                    info!("ConnectionEstablished: {peer_id} {endpoint:?} {num_established}");

                    // Match on a connection with the rendezvous server
                    if let Some(rendezvous_peer_id) = network_config.rendezvous_peer_id {
                        if peer_id == rendezvous_peer_id {
                            if let Some(rendezvous_client) =
                                swarm.behaviour_mut().rendezvous_client.as_mut()
                            {
                                trace!(
                            "Connected to rendezvous point, discovering nodes in '{NODE_NAMESPACE}' namespace ..."
                        );

                                rendezvous_client.discover(
                                    Some(rendezvous::Namespace::from_static(NODE_NAMESPACE)),
                                    None,
                                    None,
                                    rendezvous_peer_id,
                                );
                            }
                        }
                    }
                }
                SwarmEvent::Dialing(peer_id) => info!("Dialing: {peer_id}"),
                SwarmEvent::ExpiredListenAddr {
                    listener_id,
                    address,
                } => trace!("ExpiredListenAddr: {listener_id:?} {address}"),

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
                } => trace!("ListenerClosed: {listener_id:?} {addresses:?} {reason:?}"),
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
                        trace!("Registered for '{namespace}' namespace at rendezvous point {rendezvous_node} for the next {ttl} seconds")
                    }
                    rendezvous::client::Event::Discovered {
                        registrations,
                        cookie: new_cookie,
                        ..
                    } => {
                        trace!("Rendezvous point responded with peer registration data");

                        cookie.replace(new_cookie);

                        for registration in registrations {
                            for address in registration.record.addresses() {
                                let peer_id = registration.record.peer_id();

                                // Only dial remote peers discovered via rendezvous server
                                if peer_id != local_peer_id {
                                    debug!("Discovered peer {peer_id} at {address}");

                                    let p2p_suffix = Protocol::P2p(*peer_id.as_ref());
                                    let address_with_p2p = if !address
                                        .ends_with(&Multiaddr::empty().with(p2p_suffix.clone()))
                                    {
                                        address.clone().with(p2p_suffix)
                                    } else {
                                        address.clone()
                                    };

                                    debug!("Preparing to dial peer {peer_id} at {address}");

                                    if let Err(err) = swarm.dial(address_with_p2p) {
                                        warn!("Failed to dial: {}", err);
                                    }
                                }
                            }
                        }
                    }
                    rendezvous::client::Event::RegisterFailed(error) => {
                        warn!("Failed to register with rendezvous point: {error}");
                    }
                    other => trace!("Unhandled rendezvous client event: {other:?}"),
                },
                SwarmEvent::Behaviour(BehaviourEvent::RendezvousServer(event)) => match event {
                    rendezvous::server::Event::PeerRegistered { peer, registration } => {
                        trace!(
                            "Peer {peer} registered for namespace '{}'",
                            registration.namespace
                        );
                    }
                    rendezvous::server::Event::DiscoverServed {
                        enquirer,
                        registrations,
                    } => {
                        trace!(
                            "Served peer {enquirer} with {} registrations",
                            registrations.len()
                        );
                    }
                    other => trace!("Unhandled rendezvous server event: {other:?}"),
                },
                SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                    match event {
                        identify::Event::Received { peer_id, .. } => {
                            trace!("Received identify information from peer {peer_id}");

                            // Only attempt registration if the local node is running as a rendezvous client
                            if let Some(rendezvous_peer_id) = network_config.rendezvous_peer_id {
                                // Register with the rendezvous server.

                                // We call `as_mut()` on the rendezvous client network behaviour in
                                // order to get a mutable reference out of the `Toggle`
                                if let Some(rendezvous_client) =
                                    swarm.behaviour_mut().rendezvous_client.as_mut()
                                {
                                    rendezvous_client.register(
                                        rendezvous::Namespace::from_static(NODE_NAMESPACE),
                                        rendezvous_peer_id,
                                        None,
                                    );
                                }
                            }
                        }
                        identify::Event::Sent { peer_id } | identify::Event::Pushed { peer_id } => {
                            trace!(
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
                SwarmEvent::Behaviour(BehaviourEvent::RelayClient(event)) => {
                    debug!("Unhandled relay client event: {event:?}")
                }
                SwarmEvent::Behaviour(BehaviourEvent::Autonat(event)) => {
                    match event {
                        autonat::Event::StatusChanged { old, new } => {
                            trace!("NAT status changed from {:?} to {:?}", old, new);

                            if let Some(addr) = external_circuit_addr.clone() {
                                trace!("Adding external relayed listen address: {}", addr);
                                swarm.add_external_address(addr, AddressScore::Finite(1));

                                if let Some(rendezvous_peer_id) = network_config.rendezvous_peer_id
                                {
                                    // Invoke registration of relayed client address with the rendezvous server
                                    if let Some(rendezvous_client) =
                                        swarm.behaviour_mut().rendezvous_client.as_mut()
                                    {
                                        rendezvous_client.register(
                                            rendezvous::Namespace::from_static(NODE_NAMESPACE),
                                            rendezvous_peer_id,
                                            None,
                                        );
                                    }
                                }
                            }
                        }
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
