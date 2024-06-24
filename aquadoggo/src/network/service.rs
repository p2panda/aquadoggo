// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::num::NonZeroU8;
use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::rendezvous::Registration;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::SwarmEvent;
use libp2p::{dcutr, identify, mdns, relay, rendezvous, Multiaddr, PeerId, Swarm};
use log::{debug, info, trace, warn};
use tokio::task;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::{Event, P2pandaBehaviour};
use crate::network::config::Transport;
use crate::network::relay::Relay;
use crate::network::utils::{dial_known_peer, is_known_peer_address};
use crate::network::{identity, peers, swarm, utils, ShutdownHandler};
use crate::{info_or_print, NetworkConfiguration};

/// Interval at which we attempt to dial known peers and relays.
const REDIAL_INTERVAL: Duration = Duration::from_secs(20);

/// Network service which handles all networking logic for a p2panda node.
///
/// This includes:
/// - Discovering and connecting to other nodes on the local network via mDNS
/// - Discovering and connecting to other nodes via a known relay node
/// - Upgrade relayed connections to direct connections (NAT traversal)
/// - Routing replication messages to connected nodes
///
/// Can perform in "relay" mode, which means in addition to the usual node networking behaviours
/// this node will also be able to act as a relay for other nodes with restricted connectivity.
pub async fn network_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let network_config = context.config.network.clone();
    let key_pair = identity::to_libp2p_key_pair(&context.key_pair);
    let local_peer_id = key_pair.public().to_peer_id();

    info_or_print(&format!("Peer id: {local_peer_id}"));

    // The swarm can be initiated with or without "relay" capabilities.
    let mut swarm = if network_config.relay_mode {
        info!("Networking service initializing with relay capabilities...");
        swarm::build_relay_swarm(&network_config, key_pair).await?
    } else {
        info!("Networking service initializing...");
        swarm::build_client_swarm(&network_config, key_pair).await?
    };

    match network_config.transport {
        Transport::QUIC => {
            // Start listening on QUIC address. Pick a random one if the given is taken already.
            let mut listen_addr_quic = Multiaddr::empty()
                .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                .with(Protocol::Udp(network_config.port))
                .with(Protocol::QuicV1);
            if swarm.listen_on(listen_addr_quic.clone()).is_err() {
                info_or_print(&format!(
                    "QUIC port {} was already taken, try random port instead ..",
                    network_config.port
                ));

                listen_addr_quic = Multiaddr::empty()
                    .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                    .with(Protocol::Udp(0))
                    .with(Protocol::QuicV1);

                swarm.listen_on(listen_addr_quic.clone())?;
            }
        }
        Transport::TCP => {
            // Start listening on TCP address. Pick a random one if the given is taken already.
            let mut listen_address_tcp = Multiaddr::empty()
                .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                .with(Protocol::Tcp(network_config.port));
            if swarm.listen_on(listen_address_tcp.clone()).is_err() {
                info_or_print(&format!(
                    "TCP port {} was already taken, try random port instead ..",
                    network_config.port
                ));

                listen_address_tcp = Multiaddr::empty()
                    .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                    .with(Protocol::Tcp(0));

                swarm.listen_on(listen_address_tcp.clone())?;
            }
        }
    }

    info!("Network service ready!");

    // Spawn main event loop handling all p2panda and libp2p network events.
    spawn_event_loop(
        swarm,
        network_config.to_owned(),
        local_peer_id,
        shutdown,
        tx,
        tx_ready,
    )
    .await
}

/// Main loop polling the async swarm event stream and incoming service messages stream.
struct EventLoop {
    /// libp2p swarm.
    swarm: Swarm<P2pandaBehaviour>,

    /// p2panda network configuration.
    network_config: NetworkConfiguration,

    /// Our own local PeerId.
    local_peer_id: PeerId,

    /// Addresses of configured relay or direct peers mapped to discovered PeerId's.
    known_peers: HashMap<Multiaddr, PeerId>,

    /// Relays for which we have discovered a PeerId via the identify behaviour.
    relays: HashMap<PeerId, Relay>,

    /// Scheduler which triggers known peer redial attempts.
    redial_scheduler: IntervalStream,

    /// Service message channel sender.
    tx: ServiceSender,

    /// Service message channel receiver.
    rx: BroadcastStream<ServiceMessage>,

    /// Shutdown handler.
    shutdown_handler: ShutdownHandler,

    /// Did we learn our own QUIC port yet.
    learned_quic_port: bool,

    /// Did we learn our own TCP port yet.
    learned_tcp_port: bool,

    /// Did we learn our observed address yet.
    learned_observed_addr: bool,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<P2pandaBehaviour>,
        network_config: NetworkConfiguration,
        local_peer_id: PeerId,
        tx: ServiceSender,
        shutdown_handler: ShutdownHandler,
    ) -> Self {
        Self {
            swarm,
            network_config,
            redial_scheduler: IntervalStream::new(interval(REDIAL_INTERVAL)),
            local_peer_id,
            rx: BroadcastStream::new(tx.subscribe()),
            tx,
            known_peers: HashMap::new(),
            relays: HashMap::new(),
            shutdown_handler,
            learned_quic_port: false,
            learned_tcp_port: false,
            learned_observed_addr: false,
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
        tokio::time::sleep(Duration::from_millis(10)).await;

        self.shutdown_handler.set_done();
    }

    /// Main event loop handling libp2p swarm events and incoming messages from the service bus as
    /// an ongoing async stream.
    pub async fn run(mut self) {
        let mut shutdown_request_received = self.shutdown_handler.is_requested();

        loop {
            tokio::select! {
                event = self.swarm.next() => {
                    let event = event.expect("Swarm stream to be infinite");
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            if !self.learned_quic_port {
                                // Show only one QUIC address during the runtime of the node, otherwise
                                // it might get too spammy
                                if let Some(address) = utils::to_quic_address(&address) {
                                    info_or_print(&format!("Node is listening on 0.0.0.0:{} (QUIC)", address.port()));
                                    self.learned_quic_port = true;
                                }
                            }

                            if !self.learned_tcp_port {
                                // Show only one TCP address during the runtime of the node, otherwise
                                // it might get too spammy
                                if let Some(address) = utils::to_tcp_address(&address) {
                                    info_or_print(&format!("Node is listening on 0.0.0.0:{} (TCP)", address.port()));
                                    self.learned_tcp_port = true;
                                }
                            }
                        }
                        SwarmEvent::Behaviour(Event::Identify(event)) => self.handle_identify_events(&event).await,
                        SwarmEvent::Behaviour(Event::Mdns(event)) => self.handle_mdns_events(&event).await,
                        SwarmEvent::Behaviour(Event::RendezvousClient(event)) => self.handle_rendezvous_client_events(&event).await,
                        SwarmEvent::Behaviour(Event::Peers(event)) => self.handle_peers_events(&event).await,
                        SwarmEvent::Behaviour(Event::RelayClient(event)) => self.handle_relay_client_events(&event).await,
                        SwarmEvent::Behaviour(Event::Dcutr(event)) => self.handle_dcutr_events(&event).await,
                        event => self.handle_swarm_events(event).await,

                    }
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
                // The redial_scheduler emits an event every `REDIAL_INTERVAL` seconds.
                Some(_) = self.redial_scheduler.next() => {
                    self.attempt_dial_known_addresses().await;
                },
                _ = shutdown_request_received.next() => {
                    self.shutdown().await;
                }
            }
        }
    }

    /// Attempt to dial all hardcoded relay and direct node addresses. Only establishes a new connection
    /// if we are currently not connected to the target peer.
    async fn attempt_dial_known_addresses(&mut self) {
        // Attempt to dial all relay addresses.
        for relay_address in self.network_config.relay_addresses.iter_mut() {
            debug!("Dial relay at address {}", relay_address);
            dial_known_peer(
                &mut self.swarm,
                &mut self.known_peers,
                relay_address,
                self.network_config.transport,
            );
        }

        // Attempt to dial all direct peer addresses.
        for direct_node_address in self.network_config.direct_node_addresses.iter_mut() {
            debug!("Dial direct peer at address {}", direct_node_address);
            dial_known_peer(
                &mut self.swarm,
                &mut self.known_peers,
                direct_node_address,
                self.network_config.transport,
            );
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
            ServiceMessage::SentMessage(peer, peer_message) => self
                .swarm
                .behaviour_mut()
                .peers
                .send_message(peer, peer_message),
            ServiceMessage::ReplicationFailed(peer) => {
                self.swarm.behaviour_mut().peers.handle_critical_error(peer);
            }
            _ => (),
        }
    }

    async fn handle_peers_events(&mut self, event: &peers::Event) {
        match event {
            peers::Event::PeerConnected(peer) => {
                // Inform other services about new peer
                self.send_service_message(ServiceMessage::PeerConnected(*peer));
            }
            peers::Event::PeerDisconnected(peer) => {
                // Inform other services about peer leaving
                self.send_service_message(ServiceMessage::PeerDisconnected(*peer));
            }
            peers::Event::MessageReceived(peer, message) => {
                // Inform other services about received messages from peer
                self.send_service_message(ServiceMessage::ReceivedMessage(*peer, message.clone()))
            }
        }
    }

    async fn handle_rendezvous_client_events(&mut self, event: &rendezvous::client::Event) {
        match event {
            rendezvous::client::Event::Discovered {
                registrations,
                rendezvous_node,
                ..
            } => {
                for Registration { record, .. } in registrations {
                    let peer_id = record.peer_id();
                    let addresses = record.addresses();

                    debug!(
                        "Discovered {} addresses for peer {}",
                        addresses.len(),
                        peer_id
                    );

                    if peer_id != self.local_peer_id {
                        if self.swarm.is_connected(&peer_id) {
                            continue;
                        }

                        if let Some(relay_address) = self.relays.get(rendezvous_node) {
                            let peer_circuit_address =
                                relay_address.circuit_addr().with(Protocol::P2p(peer_id));

                            let opts = DialOpts::peer_id(peer_id)
                                .override_dial_concurrency_factor(
                                    NonZeroU8::new(1).expect("Is nonzero u8"),
                                )
                                .addresses(vec![peer_circuit_address])
                                .build();

                            if self.swarm.dial(opts).is_ok() {
                                debug!("Dialed peer {}", peer_id)
                            };
                        } else {
                            debug!("Discovered peer from unknown relay node")
                        };
                    }
                }
            }
            rendezvous::client::Event::Registered {
                namespace,
                rendezvous_node,
                ..
            } => {
                if let Some(relay) = self.relays.get_mut(rendezvous_node) {
                    if !relay.registered {
                        debug!("Registered on rendezvous {rendezvous_node} in namespace \"{namespace}\"");
                        relay.registered = true;
                    }
                    if relay.discover(&mut self.swarm) {
                        info!(
                            "Discovering peers in namespace \"{}\" on relay {}",
                            relay.namespace, relay.peer_id
                        );
                    };
                }
            }
            event => trace!("{event:?}"),
        }
    }

    async fn handle_identify_events(&mut self, event: &identify::Event) {
        match event {
            identify::Event::Received {
                info: identify::Info { observed_addr, .. },
                peer_id,
            } => {
                // We now learned at least one of our observed addr.
                self.learned_observed_addr = true;

                // If we don't know of the observed address a peer told us then add it to our
                // external addresses.
                if !self
                    .swarm
                    .external_addresses()
                    .any(|addr| addr == observed_addr)
                {
                    self.swarm.add_external_address(observed_addr.clone());
                }

                // Configuring known static relay and peer addresses is done by providing an ip
                // address or domain name and port. We don't yet know the peer id of the relay or
                // direct peer. Here we observe all identify events and check the addresses the
                // identified peer provides. If one matches our known addresses then we can add
                // their peer id to our address book. This is then used when dialing the peer
                // to avoid multiple connections being established to the same peer.

                // Check if the identified peer is one of our configured relay addresses.
                if let Some(relay) = self.relays.get_mut(peer_id) {
                    if !self.learned_observed_addr || !relay.told_addr {
                        return;
                    }

                    // Attempt to register with the relay.
                    match relay.register(&mut self.swarm) {
                        Ok(registered) => {
                            if registered {
                                debug!("Registration request sent to relay {}", relay.peer_id)
                            }
                        }
                        Err(e) => debug!("Error registering on relay: {}", e),
                    };
                }
            }
            identify::Event::Sent { peer_id } => {
                if let Some(relay) = self.relays.get_mut(peer_id) {
                    if !relay.told_addr {
                        debug!("Told relay {} its public address", { peer_id });
                        relay.told_addr = true;
                    }

                    if !self.learned_observed_addr || !relay.told_addr {
                        return;
                    }

                    // Attempt to register with the relay.
                    debug!("Register on relay {}", relay.peer_id);
                    match relay.register(&mut self.swarm) {
                        Ok(registered) => {
                            if registered {
                                debug!("Registration request sent to relay {}", relay.peer_id)
                            }
                        }
                        Err(e) => debug!("Error registering on relay: {}", e),
                    };
                }
            }
            event => trace!("{event:?}"),
        }
    }

    async fn handle_mdns_events(&mut self, event: &mdns::Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, _) in list {
                    debug!("mDNS discovered a new peer: {peer_id}");

                    // Dial discovered peer.
                    let dial_opts = DialOpts::peer_id(*peer_id)
                        .override_dial_concurrency_factor(NonZeroU8::new(1).expect("Is nonzero u8"))
                        .build();

                    match self.swarm.dial(dial_opts) {
                        Ok(_) => (),
                        Err(err) => debug!("Error dialing peer: {:?}", err),
                    };
                }
            }
            event => trace!("{event:?}"),
        }
    }

    async fn handle_relay_client_events(&mut self, event: &relay::client::Event) {
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } => {
                debug!("Relay {relay_peer_id} accepted circuit reservation request");

                if let Some(relay) = self.relays.get_mut(relay_peer_id) {
                    relay.reservation_accepted = true;
                    // Attempt to start discovering peers at the configured namespace.
                    if relay.discover(&mut self.swarm) {
                        info!(
                            "Discovering peers in namespace \"{}\" on relay {}",
                            relay.namespace, relay.peer_id
                        );
                    };
                }
            }
            event => trace!("{event:?}"),
        }
    }

    async fn handle_dcutr_events(&mut self, event: &dcutr::Event) {
        match &event.result {
            Ok(connection_id) => {
                info!(
                    "Connection with {}({}) upgraded to direct connection",
                    event.remote_peer_id, connection_id
                );
            }
            Err(e) => debug!("Direct connection upgrade error: {}", e),
        }
    }

    async fn handle_swarm_events(&mut self, event: SwarmEvent<Event>) {
        match event {
            SwarmEvent::ConnectionEstablished {
                endpoint,
                num_established,
                peer_id,
                ..
            } => {
                debug!(
                    "Connected to {} (1/{})",
                    endpoint.get_remote_address(),
                    num_established
                );

                // Check if the connected peer is one of our relay addresses.
                if let Some(addr) = is_known_peer_address(
                    &mut self.network_config.relay_addresses,
                    &[endpoint.get_remote_address().to_owned()],
                    self.network_config.transport,
                ) {
                    if self.relays.contains_key(&peer_id) {
                        return;
                    }

                    // Add the relay to our known peers.
                    debug!("Relay identified {peer_id} {addr}");
                    self.known_peers.insert(addr.clone(), peer_id);
                    self.relays.insert(peer_id, Relay::new(peer_id, addr));
                }

                // Check if the connected peer is one of our direct node addresses.
                if let Some(addr) = is_known_peer_address(
                    &mut self.network_config.direct_node_addresses,
                    &[endpoint.get_remote_address().to_owned()],
                    self.network_config.transport,
                ) {
                    // Add the direct node to our known peers.
                    debug!("Direct node identified {peer_id} {addr}");
                    self.known_peers.insert(addr, peer_id);
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                cause,
                ..
            } => {
                debug!(
                    "Connection closed with peer {}({}) at {}: {}",
                    peer_id,
                    connection_id,
                    endpoint.get_remote_address(),
                    cause
                        .map(|cause| cause.to_string())
                        .unwrap_or("No cause given".to_string())
                );

                // Remove this peer address from our known peers.
                self.known_peers.remove(endpoint.get_remote_address());
            }
            event => trace!("{event:?}"),
        }
    }
}

pub async fn spawn_event_loop(
    swarm: Swarm<P2pandaBehaviour>,
    network_config: NetworkConfiguration,
    local_peer_id: PeerId,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let mut shutdown_handler = ShutdownHandler::new();

    // Spawn a task to run swarm in event loop
    let event_loop = EventLoop::new(
        swarm,
        network_config,
        local_peer_id,
        tx,
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
