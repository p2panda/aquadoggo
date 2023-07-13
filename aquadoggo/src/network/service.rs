// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Event;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionError, SwarmEvent};
use libp2p::{autonat, identify, mdns, rendezvous, Multiaddr, PeerId, Swarm};
use log::{debug, info, trace, warn};
use tokio::task;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::{Behaviour, BehaviourEvent};
use crate::network::config::NODE_NAMESPACE;
use crate::network::{identity, peers, dialer, swarm, NetworkConfiguration, ShutdownHandler};

/// Network service that configures and deploys a libp2p network swarm over QUIC transports.
///
/// The swarm listens for incoming connections, dials remote nodes, manages connections and
/// executes predefined network behaviours.
pub async fn network_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let _rx = tx.subscribe();

    // Read the network configuration parameters from the application context
    let network_config = context.config.network.clone();
    let key_pair = identity::to_libp2p_key_pair(&context.key_pair);
    let local_peer_id = key_pair.public().to_peer_id();
    info!("Local peer id: {local_peer_id}");

    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_swarm(&network_config, key_pair).await?;

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
                .with(Protocol::P2p(rendezvous_peer_id))
                .with(Protocol::P2pCircuit);

            // Dialable circuit relay address for local node
            external_circuit_addr = Some(circuit_addr.clone().with(Protocol::P2p(local_peer_id)));

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

    let mut shutdown_handler = ShutdownHandler::new();

    // Spawn a task to run swarm in event loop
    let event_loop = EventLoop::new(
        swarm,
        tx,
        external_circuit_addr,
        network_config,
        shutdown_handler.clone(),
    );
    let handle = task::spawn(event_loop.run());

    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about network service being ready");
    };

    // Wait until we received the application shutdown signal or handle closed
    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    shutdown_handler.is_done().await;

    Ok(())
}

/// Main loop polling the async swarm event stream and incoming service messages stream.
struct EventLoop {
    swarm: Swarm<Behaviour>,
    tx: ServiceSender,
    rx: BroadcastStream<ServiceMessage>,
    external_circuit_addr: Option<Multiaddr>,
    network_config: NetworkConfiguration,
    shutdown_handler: ShutdownHandler,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<Behaviour>,
        tx: ServiceSender,
        external_circuit_addr: Option<Multiaddr>,
        network_config: NetworkConfiguration,
        shutdown_handler: ShutdownHandler,
    ) -> Self {
        Self {
            swarm,
            rx: BroadcastStream::new(tx.subscribe()),
            tx,
            external_circuit_addr,
            network_config,
            shutdown_handler,
        }
    }

    /// Close all connections actively.
    pub async fn shutdown(&mut self) {
        let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();

        for peer_id in peers {
            if self.swarm.disconnect_peer_id(peer_id).is_err() {
                // Silently ignore errors when disconnecting during shutdown
            }
        }

        // Wait a little bit for libp2p to actually close all connections
        tokio::time::sleep(Duration::from_millis(25)).await;

        self.shutdown_handler.set_done();
    }

    /// Main event loop handling libp2p swarm events and incoming messages from the service bus as
    /// an ongoing async stream.
    pub async fn run(mut self) {
        let mut shutdown_request_received = self.shutdown_handler.is_requested();

        loop {
            tokio::select! {
                event = self.swarm.next() => {
                    self.handle_swarm_event(event.expect("Swarm stream to be infinite")).await
                }
                event = self.rx.next() => match event {
                    Some(Ok(message)) => self.handle_service_message(message).await,
                    Some(Err(err)) => {
                        panic!("Service bus subscriber for event loop failed: {}", err);
                    }
                    // Command channel closed, thus shutting down the network event loop
                    None => {
                        return
                    },
                },
                _ = shutdown_request_received.next() => {
                    self.shutdown().await;
                }
            }
        }
    }

    /// Send a message on the communication bus to inform other services.
    fn send_service_message(&mut self, message: ServiceMessage) {
        if self.tx.send(message).is_err() {
            // Silently fail here as we don't care if the message was received at this
            // point
        }
    }

    /// Handle an incoming message via the communication bus from other services.
    async fn handle_service_message(&mut self, message: ServiceMessage) {
        match message {
            ServiceMessage::SentReplicationMessage(peer, sync_message) => {
                self.swarm
                    .behaviour_mut()
                    .peers
                    .send_message(peer, sync_message);
            }
            ServiceMessage::ReplicationFailed(peer) => {
                self.swarm.behaviour_mut().peers.handle_critical_error(peer);
            }
            _ => (),
        }
    }

    /// Handle an event coming from the libp2p swarm.
    async fn handle_swarm_event<E: std::fmt::Debug>(
        &mut self,
        event: SwarmEvent<BehaviourEvent, E>,
    ) {
        match event {
            // ~~~~~
            // Swarm
            // ~~~~~
            SwarmEvent::Dialing {
                peer_id,
                connection_id,
            } => match peer_id {
                Some(peer_id) => trace!("Dialing: {peer_id} on connection {connection_id:?}"),
                None => trace!("Dialing: unknown peer on connection {connection_id:?}"),
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                connection_id,
                ..
            } => {
                trace!("Established new connection (total {num_established}) with {peer_id} on connection {connection_id:?}");

                // Match on a connection with the rendezvous server
                if let Some(rendezvous_peer_id) = self.network_config.rendezvous_peer_id {
                    if peer_id == rendezvous_peer_id {
                        if let Some(rendezvous_client) =
                            self.swarm.behaviour_mut().rendezvous_client.as_mut()
                        {
                            trace!("Connected to rendezvous point, discovering nodes in '{NODE_NAMESPACE}' namespace ...");

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
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                cause,
                ..
            } => match cause {
                Some(ConnectionError::IO(error)) => {
                    // IO errors coming from libp2p are cumbersome to match, so we just convert
                    // them to their string representation
                    match error.to_string().as_str() {
                        "timed out" => {
                            debug!("Connection {connection_id:?} timed out with peer {peer_id}");
                        }
                        "closed by peer: 0" => {
                            // We received an `ApplicationClose` with code 0 here which means the
                            // other peer actively closed the connection
                            debug!("Connection {connection_id:?} closed with peer {peer_id}");
                        }
                        _ => {
                            warn!("Connection error occurred with peer {peer_id} on connection {connection_id:?}");
                        }
                    }
                }
                Some(ConnectionError::KeepAliveTimeout) => {
                    debug!("Connection timed out with peer {peer_id}");
                }
                Some(ConnectionError::Handler(_)) => {
                    warn!("Connection handler error occurred with peer {peer_id}");
                }
                None => {
                    debug!("Connection closed with peer {peer_id}");
                }
            },
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => trace!("Expired listen address: {listener_id:?} {address}"),
            SwarmEvent::IncomingConnection {
                connection_id,
                local_addr,
                send_back_addr,
            } => trace!("Incoming connection {connection_id:?}: {local_addr} {send_back_addr}"),
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                ..
            } => {
                warn!("Incoming connection error occurred with {local_addr} and {send_back_addr} on connection {connection_id:?}");
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                addresses,
                reason,
            } => trace!("Listener closed: {listener_id:?} {addresses:?} {reason:?}"),
            SwarmEvent::ListenerError { error, .. } => {
                warn!("Listener failed with error: {error}")
            }
            SwarmEvent::NewListenAddr {
                address,
                listener_id: _,
            } => {
                debug!("Listening on {address}");
            }
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                peer_id,
                ..
            } => match peer_id {
                Some(id) => {
                    warn!("Outgoing connection error with peer {id} occurred on connection {connection_id:?}");
                }
                None => {
                    warn!("Outgoing connection error occurred on connection {connection_id:?}");
                }
            },

            // ~~~~
            // mDNS
            // ~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                mdns::Event::Discovered(list) => {
                    for (peer_id, multiaddr) in list {
                        debug!("mDNS discovered a new peer: {peer_id}");
                        self.swarm.behaviour_mut().redial.peer_discovered(peer_id)
                    }
                }
                mdns::Event::Expired(list) => {
                    for (peer_id, _multiaddr) in list {
                        trace!("mDNS peer has expired: {peer_id}");
                        self.swarm.behaviour_mut().redial.peer_expired(peer_id)
                    }
                }
            },

            // ~~~~
            // Ping
            // ~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Ping(Event {
                connection: _,
                peer,
                result: _,
            })) => {
                trace!("Ping from: {peer}")
            }

            // ~~~~~~~~~~
            // Rendezvous
            // ~~~~~~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(event)) => match event {
                rendezvous::client::Event::Registered {
                    namespace,
                    ttl,
                    rendezvous_node,
                } => {
                    trace!("Registered for '{namespace}' namespace at rendezvous point {rendezvous_node} for the next {ttl} seconds")
                }
                rendezvous::client::Event::Discovered { registrations, .. } => {
                    trace!("Rendezvous point responded with peer registration data");

                    for registration in registrations {
                        for address in registration.record.addresses() {
                            let peer_id = registration.record.peer_id();
                            let local_peer_id = *self.swarm.local_peer_id();

                            // Only dial remote peers discovered via rendezvous server
                            if peer_id != local_peer_id {
                                debug!("Discovered peer {peer_id} at {address}");
                                self.swarm.behaviour_mut().redial.peer_discovered(peer_id);
                            }
                        }
                    }
                }
                rendezvous::client::Event::RegisterFailed { error, .. } => {
                    warn!("Failed to register with rendezvous point: {error:?}");
                }
                rendezvous::client::Event::DiscoverFailed { error, .. } => {
                    trace!("Discovery failed: {error:?}")
                }
                rendezvous::client::Event::Expired { peer } => {
                    trace!("Peer registration with rendezvous expired: {peer:?}");
                    self.swarm.behaviour_mut().redial.peer_expired(peer);
                }
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

            // ~~~~~~~~
            // Identify
            // ~~~~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                match event {
                    identify::Event::Received { peer_id, .. } => {
                        trace!("Received identify information from peer {peer_id}");

                        // Only attempt registration if the local node is running as a rendezvous client
                        if let Some(rendezvous_peer_id) = self.network_config.rendezvous_peer_id {
                            // Register with the rendezvous server.

                            // We call `as_mut()` on the rendezvous client network behaviour in
                            // order to get a mutable reference out of the `Toggle`
                            if let Some(rendezvous_client) =
                                self.swarm.behaviour_mut().rendezvous_client.as_mut()
                            {
                                match rendezvous_client.register(
                                    rendezvous::Namespace::from_static(NODE_NAMESPACE),
                                    rendezvous_peer_id,
                                    None,
                                ) {
                                    Ok(_) => (),
                                    Err(_) => {
                                        warn!("Failed to register peer: {rendezvous_peer_id}")
                                    }
                                };
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

            // ~~~~~
            // Relay
            // ~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::RelayServer(event)) => {
                trace!("Unhandled relay server event: {event:?}")
            }
            SwarmEvent::Behaviour(BehaviourEvent::RelayClient(event)) => {
                trace!("Unhandled relay client event: {event:?}")
            }

            // ~~~~~~~
            // AutoNAT
            // ~~~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Autonat(event)) => {
                match event {
                    autonat::Event::StatusChanged { old, new } => {
                        trace!("NAT status changed from {:?} to {:?}", old, new);

                        if let Some(addr) = self.external_circuit_addr.clone() {
                            trace!("Adding external relayed listen address: {}", addr);
                            self.swarm.add_external_address(addr);

                            if let Some(rendezvous_peer_id) = self.network_config.rendezvous_peer_id
                            {
                                // Invoke registration of relayed client address with the rendezvous server
                                if let Some(rendezvous_client) =
                                    self.swarm.behaviour_mut().rendezvous_client.as_mut()
                                {
                                    match rendezvous_client.register(
                                        rendezvous::Namespace::from_static(NODE_NAMESPACE),
                                        rendezvous_peer_id,
                                        None,
                                    ) {
                                        Ok(_) => (),
                                        Err(_) => {
                                            warn!("Failed to register peer: {rendezvous_peer_id}")
                                        }
                                    };
                                }
                            }
                        }
                    }
                    autonat::Event::InboundProbe(_) | autonat::Event::OutboundProbe(_) => (),
                }
            }

            // ~~~~~~
            // Limits
            // ~~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Limits(event)) => {
                debug!("Unhandled connection limit event: {event:?}")
            }

            // ~~~~~~~~~~~~~
            // p2panda peers
            // ~~~~~~~~~~~~~
            SwarmEvent::Behaviour(BehaviourEvent::Peers(event)) => match event {
                peers::Event::PeerConnected(peer) => {
                    // Inform other services about new peer
                    self.send_service_message(ServiceMessage::PeerConnected(peer));
                }
                peers::Event::PeerDisconnected(peer) => {
                    // Inform other services about peer leaving
                    self.send_service_message(ServiceMessage::PeerDisconnected(peer));
                }
                peers::Event::MessageReceived(peer, message) => {
                    // Inform other services about received messages from peer
                    self.send_service_message(ServiceMessage::ReceivedReplicationMessage(
                        peer, message,
                    ))
                }
            },

            SwarmEvent::Behaviour(BehaviourEvent::Redial(event)) => match event {
                dialer::Event::Dial(peer_id) => {
                    match self.swarm.dial(
                        DialOpts::peer_id(peer_id)
                        .condition(PeerCondition::Disconnected)
                        .condition(PeerCondition::NotDialing)
                        .build(),
                    ) {
                        Ok(_) => debug!("Dialing peer: {peer_id}"),
                        Err(_) => warn!("Error dialing peer: {peer_id}"),
                    };
                }
            },
        }
    }
}
