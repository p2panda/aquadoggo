// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionError, NetworkBehaviour, SwarmEvent};
use libp2p::{autonat, identify, mdns, relay, rendezvous, Multiaddr, PeerId, Swarm};
use log::{debug, info, trace, warn};
use tokio::task;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::Event;
use crate::network::config::NODE_NAMESPACE;
use crate::network::{dialer, identity, peers, swarm, NetworkConfiguration, ShutdownHandler};

use super::behaviour::P2pandaBehaviour;

pub async fn spawn_event_loop(
    swarm: Swarm<P2pandaBehaviour>,
    local_peer_id: PeerId,
    network_config: NetworkConfiguration,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let mut shutdown_handler = ShutdownHandler::new();

    // Spawn a task to run swarm in event loop
    let event_loop = EventLoop::new(
        swarm,
        local_peer_id,
        tx,
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

pub async fn network_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Read the network configuration parameters from the application context
    let network_config = context.config.network.clone();
    let key_pair = identity::to_libp2p_key_pair(&context.key_pair);
    let local_peer_id = key_pair.public().to_peer_id();
    info!("Local peer id: {local_peer_id}");

    if network_config.relay_server_enabled {
        let swarm = relay(&network_config, key_pair).await?;
        spawn_event_loop(swarm, local_peer_id, network_config, shutdown, tx, tx_ready).await
    } else {
        let swarm: Swarm<P2pandaBehaviour> = client(&network_config, key_pair).await?;
        spawn_event_loop(swarm, local_peer_id, network_config, shutdown, tx, tx_ready).await
    }
}

pub async fn client(
    network_config: &NetworkConfiguration,
    key_pair: libp2p::identity::Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let quic_port = network_config.quic_port.unwrap_or(0);
    let relay_address = network_config.relay_address.clone();

    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_client_swarm(&network_config, key_pair).await?;

    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(quic_port))
        .with(Protocol::QuicV1);
    match swarm.listen_on(listen_addr_quic.clone()) {
        Ok(_) => (),
        Err(_) => warn!("Failed to listen on address: {listen_addr_quic:?}"),
    };

    if let Some(relay_address) = relay_address {
        let rendezvous_point = network_config.clone().relay_peer_id.unwrap();
        let rendezvous_address = network_config.clone().rendezvous_address.unwrap();

        // Connect to the relay server. Not for the reservation or relayed connection, but to (a) learn
        // our local public address and (b) enable a freshly started relay to learn its public
        // address.

        match swarm.dial(rendezvous_address.clone()) {
            Ok(_) => (),
            Err(_) => warn!("Failed to dial relay: {rendezvous_address:?}"),
        };

        // Wait to get confirmation that we told the relay node it's public address and that they told
        // us ours.

        let mut learned_observed_addr = false;
        let mut told_relay_observed_addr = false;

        loop {
            match swarm.next().await.unwrap() {
                SwarmEvent::Behaviour(Event::Identify(identify::Event::Sent { .. })) => {
                    info!("Told relay its public address.");
                    told_relay_observed_addr = true;
                }
                SwarmEvent::Behaviour(Event::Identify(identify::Event::Received {
                    info: identify::Info { observed_addr, .. },
                    ..
                })) => {
                    info!("Relay told us our public address: {:?}", observed_addr);
                    swarm.add_external_address(observed_addr);
                    learned_observed_addr = true;
                }
                event => debug!("{event:?}"),
            }

            if learned_observed_addr && told_relay_observed_addr {
                break;
            }
        }

        // Now we have received our external address, and we know the relay has too, listen on our
        // relay circuit address.
        let circuit_relay_address = relay_address.clone().with(Protocol::P2pCircuit);
        swarm.listen_on(circuit_relay_address.clone()).unwrap();

        // Register in the `NODE_NAMESPACE` on the rendezvous server. Doing this will mean that we can
        // discover other peers also registered to the same rendezvous server and namespace.
        swarm
            .behaviour_mut()
            .rendezvous_client
            .as_mut()
            .unwrap()
            .register(
                rendezvous::Namespace::from_static(NODE_NAMESPACE),
                rendezvous_point,
                None, // Default ttl is 7200s
            )
            .unwrap();

        // Wait to get confirmation that our registration on the rendezvous server at namespace
        // `NODE_NAMESPACE` was successful and that the relay server has accepted our reservation.

        let mut rendezvous_registered = false;
        let mut relay_reservation_accepted = false;

        loop {
            match swarm.next().await.expect("Infinite Stream") {
                SwarmEvent::Behaviour(Event::RelayClient(
                    relay::client::Event::ReservationReqAccepted { .. },
                )) => {
                    info!("Relay circuit reservation request accepted");
                    relay_reservation_accepted = true;
                }
                SwarmEvent::Behaviour(Event::RendezvousClient(
                    rendezvous::client::Event::Registered { namespace, .. },
                )) => {
                    info!("Registered on rendezvous in namespace \"{namespace}\"");
                    rendezvous_registered = true;
                }
                SwarmEvent::ConnectionClosed { .. } => {
                    // Listening on a relay circuit address opens a connection to the relay node, this
                    // doesn't always succeed first time though, so we catch if the connection closed
                    // and retry listening.
                    warn!("Relay circuit connection closed, re-attempting listening on: {circuit_relay_address}");
                    swarm.listen_on(circuit_relay_address.clone()).unwrap();
                }
                event => debug!("{event:?}"),
            }

            if relay_reservation_accepted && rendezvous_registered {
                break;
            }
        }

        // Now request to discover other peers in `NODE_NAMESPACE`.
        info!("Discovering peers in namespace \"{NODE_NAMESPACE}\"",);
        swarm
            .behaviour_mut()
            .rendezvous_client
            .as_mut()
            .unwrap()
            .discover(
                Some(rendezvous::Namespace::new(NODE_NAMESPACE.to_string()).unwrap()),
                None,
                None,
                rendezvous_point,
            );
    }

    // All swarm setup complete and we are connected to the network.
    info!("Node initialized in client mode");

    Ok(swarm)
}

pub async fn relay(
    network_config: &NetworkConfiguration,
    key_pair: libp2p::identity::Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let quic_port = network_config.quic_port.unwrap_or(2022);
    let mut swarm = swarm::build_relay_swarm(&network_config, key_pair).await?;

    let listen_addr_tcp = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Tcp(2030));
    swarm.listen_on(listen_addr_tcp)?;

    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(quic_port))
        .with(Protocol::QuicV1);
    swarm.listen_on(listen_addr_quic)?;

    info!("Relay initialized");

    Ok(swarm)
}

/// Main loop polling the async swarm event stream and incoming service messages stream.
struct EventLoop {
    swarm: Swarm<P2pandaBehaviour>,
    local_peer_id: PeerId,
    tx: ServiceSender,
    rx: BroadcastStream<ServiceMessage>,
    network_config: NetworkConfiguration,
    shutdown_handler: ShutdownHandler,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<P2pandaBehaviour>,
        local_peer_id: PeerId,
        tx: ServiceSender,
        network_config: NetworkConfiguration,
        shutdown_handler: ShutdownHandler,
    ) -> Self {
        Self {
            swarm,
            local_peer_id,
            rx: BroadcastStream::new(tx.subscribe()),
            tx,
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

        if let Some(peers) = self.swarm.behaviour_mut().peers.as_mut() {
            peers.run();
        };

        loop {
            tokio::select! {
                event = self.swarm.next() => {
                    let event = event.expect("Swarm stream to be infinite");
                    self.handle_identify_event_events(&event).await;

                    if self.swarm.behaviour().peers.is_enabled() {
                        self.handle_mdns_discovery_events(&event).await;
                        self.handle_rendezvous_discovery_events(&event).await;
                        self.handle_dcutr_events(&event).await;
                        self.handle_peer_events(&event).await;
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
                if let Some(peers) = self.swarm.behaviour_mut().peers.as_mut() {
                    peers.send_message(peer, sync_message)
                }
            }
            ServiceMessage::ReplicationFailed(peer) => {
                if let Some(peers) = self.swarm.behaviour_mut().peers.as_mut() {
                    peers.handle_critical_error(peer);
                }
            }
            _ => (),
        }
    }

    async fn handle_peer_events<E: std::fmt::Debug>(&mut self, event: &SwarmEvent<Event, E>) {
        match event {
            // ~~~~~~~~~~~~~
            // p2panda peers
            // ~~~~~~~~~~~~~
            SwarmEvent::Behaviour(Event::Peers(event)) => match event {
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
                    self.send_service_message(ServiceMessage::ReceivedReplicationMessage(
                        *peer,
                        message.clone(),
                    ))
                }
            },
            _ => (),
        }
    }

    async fn handle_rendezvous_discovery_events<E: std::fmt::Debug>(
        &mut self,
        event: &SwarmEvent<Event, E>,
    ) {
        match event {
            // ~~~~~~~~~~~~~~~~~~~~
            // rendezvous discovery
            // ~~~~~~~~~~~~~~~~~~~~
            SwarmEvent::Behaviour(Event::RendezvousClient(
                rendezvous::client::Event::Discovered { registrations, .. },
            )) => {
                info!("Discovered peers registered at rendezvous: {registrations:?}",);

                for registration in registrations {
                    for address in registration.record.addresses() {
                        let peer_id = registration.record.peer_id();
                        if peer_id != self.local_peer_id {
                            info!("Add new peer to address book: {} {}", peer_id, address);

                            let addr = self
                                .network_config
                                .clone()
                                .relay_address
                                .unwrap()
                                .clone()
                                .with(Protocol::P2pCircuit)
                                .with(Protocol::P2p(PeerId::from(peer_id).into()));

                            if let Some(dialer) = self.swarm.behaviour_mut().dialer.as_mut() {
                                dialer.add_peer(peer_id, addr);
                                dialer.dial_peer(peer_id)
                            };
                        }
                    }
                }
            }
            _ => (),
        }
    }

    async fn handle_identify_event_events<E: std::fmt::Debug>(
        &mut self,
        event: &SwarmEvent<Event, E>,
    ) {
        match event {
            SwarmEvent::Behaviour(Event::Identify(identify::Event::Received {
                info: identify::Info { observed_addr, .. },
                ..
            })) => {
                info!("Observed external address reported: {observed_addr}");
                if !self
                    .swarm
                    .external_addresses()
                    .any(|addr| addr == observed_addr)
                {
                    self.swarm.add_external_address(observed_addr.clone());
                }
            }
            _ => (),
        }
    }

    async fn handle_mdns_discovery_events<E: std::fmt::Debug>(
        &mut self,
        event: &SwarmEvent<Event, E>,
    ) {
        match event {
            SwarmEvent::Behaviour(Event::Mdns(event)) => match event {
                mdns::Event::Discovered(list) => {
                    for (peer_id, addr) in list {
                        debug!("mDNS discovered a new peer: {peer_id}");

                        if let Some(dialer) = self.swarm.behaviour_mut().dialer.as_mut() {
                            dialer.add_peer(*peer_id, addr.to_owned());
                            dialer.dial_peer(*peer_id);
                        };
                    }
                }
                mdns::Event::Expired(list) => {
                    for (peer_id, _multiaddr) in list {
                        trace!("mDNS peer has expired: {peer_id}");
                    }
                }
            },
            _ => (),
        }
    }

    async fn handle_dcutr_events<E: std::fmt::Debug>(&mut self, event: &SwarmEvent<Event, E>) {
        match event {
            SwarmEvent::Behaviour(Event::Dcutr(event)) => {
                debug!("{:?}", event)
            }
            _ => (),
        }
    }
}
