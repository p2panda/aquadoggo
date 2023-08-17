// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, mdns, relay, rendezvous, Multiaddr, PeerId, Swarm};
use log::{debug, info, trace, warn};
use tokio::task;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::Event;
use crate::network::config::NODE_NAMESPACE;
use crate::network::{identity, peers, swarm, NetworkConfiguration, ShutdownHandler};

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
    let mut network_config = context.config.network.clone();
    let key_pair = identity::to_libp2p_key_pair(&context.key_pair);
    let local_peer_id = key_pair.public().to_peer_id();
    info!("Local peer id: {local_peer_id}");

    if network_config.relay_server_enabled {
        let swarm = relay(&network_config, key_pair).await?;
        spawn_event_loop(swarm, local_peer_id, network_config, shutdown, tx, tx_ready).await
    } else {
        let swarm: Swarm<P2pandaBehaviour> = client(&mut network_config, key_pair).await?;
        spawn_event_loop(swarm, local_peer_id, network_config, shutdown, tx, tx_ready).await
    }
}

pub async fn client(
    network_config: &mut NetworkConfiguration,
    key_pair: libp2p::identity::Keypair,
) -> Result<Swarm<P2pandaBehaviour>> {
    let quic_port = network_config.quic_port.unwrap_or(0);

    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_client_swarm(&network_config, key_pair).await?;

    swarm.behaviour_mut().peers.stop();

    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Tcp(0)),
    )?;

    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Udp(quic_port))
            .with(Protocol::QuicV1),
    )?;

    if let Some(mut relay_addr) = network_config.relay_addr.clone() {
        // Connect to the relay server. Not for the reservation or relayed connection, but to (a) learn
        // our local public address and (b) enable a freshly started relay to learn its public
        // address.

        swarm.dial(relay_addr.clone())?;

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
                    peer_id,
                })) => {
                    info!("Relay told us our public address: {:?}", observed_addr);
                    let _ = relay_addr.pop();
                    relay_addr.push(Protocol::P2p(peer_id));
                    swarm.add_external_address(observed_addr);

                    learned_observed_addr = true;
                    network_config.relay_peer_id = Some(peer_id);
                    network_config.relay_addr = Some(relay_addr.clone());
                }
                event => debug!("{event:?}"),
            }

            if learned_observed_addr && told_relay_observed_addr {
                break;
            }
        }

        let relay_peer_id = network_config
            .relay_peer_id
            .expect("Received relay peer id");

        // Now we have received our external address, and we know the relay has too, listen on our
        // relay circuit address.
        let circuit_addr = relay_addr.clone().with(Protocol::P2pCircuit);
        swarm.listen_on(circuit_addr.clone())?;

        // Register in the `NODE_NAMESPACE` on the rendezvous server. Doing this will mean that we can
        // discover other peers also registered to the same rendezvous server and namespace.
        swarm
            .behaviour_mut()
            .rendezvous_client
            .as_mut()
            .unwrap()
            .register(
                rendezvous::Namespace::from_static(NODE_NAMESPACE),
                relay_peer_id,
                None, // Default ttl is 7200s
            )?;

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
                    warn!(
                        "Relay circuit connection closed, re-attempting listening on: {:?}",
                        circuit_addr
                    );
                    swarm.listen_on(circuit_addr.clone())?;
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
                relay_peer_id,
            );

        let opts = DialOpts::peer_id(relay_peer_id)
            .condition(PeerCondition::Always)
            .build();

        match swarm.dial(opts) {
            Ok(_) => (),
            Err(err) => debug!("Error dialing peer: {:?}", err),
        };
    }

    swarm.behaviour_mut().peers.restart();

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
        .with(Protocol::Tcp(0));
    swarm.listen_on(listen_addr_tcp)?;

    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(quic_port))
        .with(Protocol::QuicV1);
    swarm.listen_on(listen_addr_quic)?;

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

        loop {
            tokio::select! {
                event = self.swarm.next() => {
                    let event = event.expect("Swarm stream to be infinite");
                    match event {
                        SwarmEvent::Behaviour(Event::Identify(event)) => self.handle_identify_events(&event).await,
                        SwarmEvent::Behaviour(Event::Mdns(event)) => self.handle_mdns_events(&event).await,
                        SwarmEvent::Behaviour(Event::RendezvousClient(event)) => self.handle_rendezvous_client_events(&event).await,
                        SwarmEvent::Behaviour(Event::Peers(event)) => self.handle_peers_events(&event).await,
                        event => trace!("{event:?}")

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
            ServiceMessage::SentReplicationMessage(peer, sync_message) => self
                .swarm
                .behaviour_mut()
                .peers
                .send_message(peer, sync_message),
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
                self.send_service_message(ServiceMessage::ReceivedReplicationMessage(
                    *peer,
                    message.clone(),
                ))
            }
        }
    }

    async fn handle_rendezvous_client_events(&mut self, event: &rendezvous::client::Event) {
        match event {
            rendezvous::client::Event::Discovered { registrations, .. } => {
                info!("Discovered peers registered at rendezvous: {registrations:?}",);

                for registration in registrations {
                    for address in registration.record.addresses() {
                        let peer_id = registration.record.peer_id();
                        if peer_id != self.local_peer_id {
                            if let Some(relay_addr) = &self.network_config.relay_addr {
                                info!("Add new peer to address book: {} {}", peer_id, address);

                                let peer_circuit_addr = relay_addr
                                    .clone()
                                    .with(Protocol::P2pCircuit)
                                    .with(Protocol::P2p(PeerId::from(peer_id).into()));

                                match self.swarm.dial(peer_circuit_addr) {
                                    Ok(_) => (),
                                    Err(err) => debug!("Error dialing peer: {:?}", err),
                                };
                            }
                        }
                    }
                }
            }
            event => trace!("{event:?}"),
        }
    }

    async fn handle_identify_events(&mut self, event: &identify::Event) {
        match event {
            identify::Event::Received {
                info: identify::Info { observed_addr, .. },
                ..
            } => {
                info!("Observed external address reported: {observed_addr}");
                if !self
                    .swarm
                    .external_addresses()
                    .any(|addr| addr == observed_addr)
                {
                    self.swarm.add_external_address(observed_addr.clone());
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

                    match self.swarm.dial(*peer_id) {
                        Ok(_) => (),
                        Err(err) => debug!("Error dialing peer: {:?}", err),
                    };
                }
            }
            event => trace!("{event:?}"),
        }
    }
}
