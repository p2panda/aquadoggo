// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::num::NonZeroU8;
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
use crate::network::behaviour::{Event, P2pandaBehaviour};
use crate::network::config::NODE_NAMESPACE;
use crate::network::{identity, peers, swarm, utils, ShutdownHandler};

const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
    let network_config = &context.config.network;
    let key_pair = identity::to_libp2p_key_pair(&context.key_pair);
    let local_peer_id = key_pair.public().to_peer_id();

    println!("Peer id: {local_peer_id}");

    // The swarm can be initiated with or without "relay" capabilities.
    let mut swarm = if network_config.relay_mode {
        info!("Networking service initializing with relay capabilities...");
        let mut swarm = swarm::build_relay_swarm(network_config, key_pair).await?;

        // Start listening on tcp address.
        let listen_addr_tcp = Multiaddr::empty()
            .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Tcp(0));
        swarm.listen_on(listen_addr_tcp)?;

        swarm
    } else {
        info!("Networking service initializing...");
        swarm::build_client_swarm(network_config, key_pair).await?
    };

    // Start listening on QUIC address. Pick a random one if the given is taken already.
    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(network_config.quic_port))
        .with(Protocol::QuicV1);
    if swarm.listen_on(listen_addr_quic).is_err() {
        let random_port_addr = Multiaddr::empty()
            .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Udp(0))
            .with(Protocol::QuicV1);
        println!(
            "QUIC port {} was already taken, try random port instead ..",
            network_config.quic_port
        );
        swarm.listen_on(random_port_addr)?;
    }

    // If relay node addresses were provided, then connect to each and perform necessary setup before we
    // run the main event loop.
    //
    // We are not connecting to other relays when in relay mode (is this even supported by libp2p)?
    // See related issue: https://github.com/p2panda/aquadoggo/issues/529
    let mut connected_relays = HashMap::new();
    if !network_config.relay_addresses.is_empty() && !network_config.relay_mode {
        // First we need to stop the "peers" behaviour.
        //
        // We do this so that the connections we create during initialization do not trigger
        // replication sessions, which could leave the node in a strange state.
        swarm.behaviour_mut().peers.disable();

        for relay_address in network_config.relay_addresses.clone() {
            let mut address = utils::to_multiaddress(&relay_address);
            info!("Connecting to relay node {}", address);

            // Attempt to connect to the relay node, we give this a 5 second timeout so as not to
            // get stuck if one relay is unreachable.
            if let Ok(result) = tokio::time::timeout(
                RELAY_CONNECT_TIMEOUT,
                connect_to_relay(&mut swarm, &mut address),
            )
            .await
            {
                match result {
                    // If the connection is successful then add this node to our map of connected relays.
                    Ok((relay_peer_id, relay_address)) => {
                        connected_relays.insert(relay_peer_id, relay_address);
                    }
                    Err(_) => info!("Failed to connect to relay node"),
                }
            } else {
                info!("Relay connection attempt failed: connection timeout")
            };
        }

        // Finally we want to dial any relay nodes we connected to _again_, this time in order to
        // initiate replication with it, which will happen automatically once the connection succeeds.
        // And then begin discovering peers in `NODE_NAMESPACE` on each relay node in order to begin
        // replicating with them too.
        //
        // @TODO: This is a workaround since we don't have a way yet to start replication _not_ at the
        // same time the connection gets successfully established. We should fix this and remove the
        // second, potentially redundant dial.
        //
        // See related issue: https://github.com/p2panda/aquadoggo/issues/507

        // First restart the "peers" behaviour in order to handle the expected messages
        swarm.behaviour_mut().peers.enable();

        for relay_peer_id in connected_relays.keys() {
            let opts = DialOpts::peer_id(*relay_peer_id)
                .condition(PeerCondition::Always) // There is an existing connection, so we force dial here.
                .build();
            match swarm.dial(opts) {
                Ok(_) => (),
                Err(err) => debug!("Error dialing peer: {:?}", err),
            };

            // Now request to discover other peers in `NODE_NAMESPACE`.
            info!("Discovering peers in namespace \"{NODE_NAMESPACE}\"",);
            swarm
                .behaviour_mut()
                .rendezvous_client
                .as_mut()
                .expect("Relay client exists as we a relay address was provided")
                .discover(
                    Some(
                        rendezvous::Namespace::new(NODE_NAMESPACE.to_string())
                            .expect("Valid namespace"),
                    ),
                    None,
                    None,
                    *relay_peer_id,
                );
        }
    }

    // Dial all nodes we want to directly connect to.
    for direct_node_address in &network_config.direct_node_addresses {
        let address = utils::to_multiaddress(&direct_node_address);
        info!("Connecting to node @ {}", address);

        let opts = DialOpts::unknown_peer_id().address(address.clone()).build();

        match swarm.dial(opts) {
            Ok(_) => (),
            Err(err) => debug!("Error dialing node: {:?}", err),
        };
    }

    info!("Network service ready!");

    // Spawn main event loop handling all p2panda and libp2p network events.
    spawn_event_loop(
        swarm,
        local_peer_id,
        connected_relays,
        shutdown,
        tx,
        tx_ready,
    )
    .await
}

/// Connect to a relay node, confirm exchange identity information and wait to listen on our
/// circuit relay address.
pub async fn connect_to_relay(
    swarm: &mut Swarm<P2pandaBehaviour>,
    relay_address: &mut Multiaddr,
) -> Result<(PeerId, Multiaddr)> {
    // Connect to the relay server. Not for the reservation or relayed connection, but to (a) learn
    // our local public address and (b) enable a freshly started relay to learn its public address.
    swarm.dial(relay_address.clone())?;

    // Wait to get confirmation that we told the relay node its public address and that they told
    // us ours.
    let mut learned_observed_addr = false;
    let mut told_relay_observed_addr = false;
    let mut learned_relay_peer_id: Option<PeerId> = None;

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
                debug!("Relay told us our public address: {:?}", observed_addr);

                // Add the newly learned address to our external addresses.
                swarm.add_external_address(observed_addr);

                // Now that we have a reply from the relay node we can add their peer id to the
                // relay address.

                // Pop off the "p2p" protocol.
                let _ = relay_address.pop();

                // Add it back on again with the relay nodes peer id included.
                relay_address.push(Protocol::P2p(peer_id));

                // Update values on the config.
                learned_relay_peer_id = Some(peer_id);

                // All done, we've learned our external address successfully.
                learned_observed_addr = true;
            }
            event => debug!("{event:?}"),
        }

        if learned_observed_addr && told_relay_observed_addr {
            break;
        }
    }

    // We know the relays peer address was learned in the above step so we unwrap it here.
    let relay_peer_id = learned_relay_peer_id.expect("Received relay peer id");

    // Now we have received our external address, and we know the relay has too, listen on our
    // relay circuit address.
    let circuit_address = relay_address.clone().with(Protocol::P2pCircuit);
    swarm.listen_on(circuit_address.clone())?;

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
                // doesn't always succeed first time though, so we catch here when the connection closed
                // and retry listening.
                warn!(
                    "Relay circuit connection closed, re-attempting listening on: {:?}",
                    circuit_address
                );

                // After a short wait.
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Listen again.
                swarm.listen_on(circuit_address.clone())?;
            }
            event => debug!("{event:?}"),
        }

        if relay_reservation_accepted && rendezvous_registered {
            break;
        }
    }

    Ok((relay_peer_id, relay_address.clone()))
}

/// Main loop polling the async swarm event stream and incoming service messages stream.
struct EventLoop {
    swarm: Swarm<P2pandaBehaviour>,
    local_peer_id: PeerId,
    tx: ServiceSender,
    rx: BroadcastStream<ServiceMessage>,
    relay_addresses: HashMap<PeerId, Multiaddr>,
    shutdown_handler: ShutdownHandler,
    learned_port: bool,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<P2pandaBehaviour>,
        local_peer_id: PeerId,
        tx: ServiceSender,
        relay_addresses: HashMap<PeerId, Multiaddr>,
        shutdown_handler: ShutdownHandler,
    ) -> Self {
        Self {
            swarm,
            local_peer_id,
            rx: BroadcastStream::new(tx.subscribe()),
            tx,
            relay_addresses,
            shutdown_handler,
            learned_port: false,
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
                        SwarmEvent::NewListenAddr { address, .. } => {
                            if self.learned_port {
                                continue;
                            }

                            // Show only one QUIC address during the runtime of the node, otherwise
                            // it might get too spammy
                            if let Some(address) = utils::to_quic_address(&address) {
                                println!("Node is listening on 0.0.0.0:{}", address.port());
                                self.learned_port = true;
                            }
                        }
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
                debug!("Discovered peers registered at rendezvous: {registrations:?}",);

                for registration in registrations {
                    for address in registration.record.addresses() {
                        let peer_id = registration.record.peer_id();
                        if peer_id != self.local_peer_id {
                            debug!("Add new peer to address book: {} {}", peer_id, address);

                            if let Some(relay_address) = self.relay_addresses.get(rendezvous_node) {
                                let peer_circuit_address = relay_address
                                    .clone()
                                    .with(Protocol::P2pCircuit)
                                    .with(Protocol::P2p(peer_id));

                                match self.swarm.dial(peer_circuit_address) {
                                    Ok(_) => (),
                                    Err(err) => debug!("Error dialing peer: {:?}", err),
                                };
                            } else {
                                debug!("Discovered peer from unknown relay node")
                            };
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
                debug!("Observed external address reported: {observed_addr}");
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

                    let dial_opts = DialOpts::peer_id(*peer_id)
                        .condition(PeerCondition::Disconnected)
                        .condition(PeerCondition::NotDialing)
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
}

pub async fn spawn_event_loop(
    swarm: Swarm<P2pandaBehaviour>,
    local_peer_id: PeerId,
    relay_addresses: HashMap<PeerId, Multiaddr>,
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
        relay_addresses,
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
