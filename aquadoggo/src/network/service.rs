// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Event;
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
use crate::network::behaviour::{ClientBehaviour, RelayBehaviour, RelayBehaviourEvent};
use crate::network::config::NODE_NAMESPACE;
use crate::network::{dialer, identity, peers, swarm, NetworkConfiguration, ShutdownHandler};

use super::behaviour::ClientBehaviourEvent;

pub async fn spawn_event_loop<T>(
    swarm: Swarm<T>,
    network_config: NetworkConfiguration,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()>
where
    T: NetworkBehaviour + Send,
    <T as NetworkBehaviour>::ToSwarm: std::fmt::Debug,
{
    let mut shutdown_handler = ShutdownHandler::new();

    // Spawn a task to run swarm in event loop
    let event_loop = EventLoop::new(swarm, tx, network_config, shutdown_handler.clone());
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

    match &network_config.relay_address {
        Some(_) => {
            let swarm = client(&network_config, key_pair).await?;
            spawn_event_loop(swarm, network_config, shutdown, tx, tx_ready).await
        }
        None => {
            let swarm = relay(&network_config, key_pair).await?;
            spawn_event_loop(swarm, network_config, shutdown, tx, tx_ready).await
        }
    }
}

pub async fn client(
    network_config: &NetworkConfiguration,
    key_pair: libp2p::identity::Keypair,
) -> Result<Swarm<ClientBehaviour>> {
    let local_peer_id = key_pair.public().to_peer_id();
    let relay_address = network_config.relay_address.clone().unwrap();
    let rendezvous_point = network_config.relay_peer_id.unwrap();

    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_client_swarm(&network_config, key_pair).await?;

    // Listen on all interfaces
    let listen_addr_tcp = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Tcp(0));
    match swarm.listen_on(listen_addr_tcp.clone()) {
        Ok(_) => (),
        Err(_) => warn!("Failed to listen on address: {listen_addr_tcp:?}"),
    };

    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(0))
        .with(Protocol::QuicV1);
    match swarm.listen_on(listen_addr_quic.clone()) {
        Ok(_) => (),
        Err(_) => warn!("Failed to listen on address: {listen_addr_quic:?}"),
    };

    // Wait to listen on all interfaces.
    let mut delay = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            event = swarm.next() => {
                match event.unwrap() {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {:?}", address);
                    }
                    event => panic!("{event:?}"),
                }
            }
            _ = delay.tick() => {
                // Likely listening on all interfaces now, thus continuing by breaking the loop.
                break;
            }
        }
    }

    // Connect to the relay server. Not for the reservation or relayed connection, but to (a) learn
    // our local public address and (b) enable a freshly started relay to learn its public address.
    match swarm.dial(relay_address.clone()) {
        Ok(_) => (),
        Err(_) => warn!("Failed to dial relay: {relay_address:?}"),
    };

    let mut learned_observed_addr = false;
    let mut told_relay_observed_addr = false;
    let mut rendezvous_registered = false;
    let mut dial_discovered = false;
    let mut relay_request_accepted = false;
    let mut regs = vec![];

    loop {
        match swarm.next().await.unwrap() {
            SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(identify::Event::Sent {
                ..
            })) => {
                info!("Told relay its public address.");
                told_relay_observed_addr = true;
            }
            SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(identify::Event::Received {
                info: identify::Info { observed_addr, .. },
                ..
            })) => {
                info!("Relay told us our public address: {:?}", observed_addr);
                swarm.add_external_address(observed_addr);
                learned_observed_addr = true;

                // default ttl is 7200s
                match swarm.behaviour_mut().rendezvous_client.register(
                    rendezvous::Namespace::from_static(NODE_NAMESPACE),
                    rendezvous_point,
                    None,
                ) {
                    Ok(_) => {
                    },
                    Err(err) => warn!(
                        "Failed to register on rendezvous server in namespace \"{NODE_NAMESPACE}\": {err:?}"
                    ),
                };

                swarm
                    .listen_on(relay_address.clone().with(Protocol::P2pCircuit))
                    .unwrap();
            }
            SwarmEvent::Behaviour(ClientBehaviourEvent::RelayClient(
                relay::client::Event::ReservationReqAccepted { relay_peer_id, .. },
            )) => {
                info!("Relay reservation request accepted: {relay_peer_id:?}");

                relay_request_accepted = true;
            }

            SwarmEvent::Behaviour(ClientBehaviourEvent::RendezvousClient(
                rendezvous::client::Event::Registered { namespace, .. },
            )) => {
                info!("Registered on rendezvous in namespace \"{namespace}\"");
                rendezvous_registered = true;

                info!(
                    "Connected to rendezvous point, discovering nodes in '{}' namespace ...",
                    NODE_NAMESPACE
                );

                swarm.behaviour_mut().rendezvous_client.discover(
                    Some(rendezvous::Namespace::new(NODE_NAMESPACE.to_string()).unwrap()),
                    None,
                    None,
                    rendezvous_point,
                );
            }
            // SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == rendezvous_point => {}
            SwarmEvent::Behaviour(ClientBehaviourEvent::RendezvousClient(
                rendezvous::client::Event::Discovered { registrations, .. },
            )) => {
                info!("Discovered peers registered at rendezvous: {registrations:?}",);

                dial_discovered = true;
                regs = registrations;
            }
            event => debug!("{event:?}"),
        }

        if learned_observed_addr
            && told_relay_observed_addr
            && rendezvous_registered
            && dial_discovered
            && relay_request_accepted
        {
            break;
        }
    }

    info!("Node initialized in client mode");

    for registration in regs {
        for address in registration.record.addresses() {
            let peer = registration.record.peer_id();
            if peer != local_peer_id {
                info!("Dialing peer {} at {}", peer, address);

                let opts = DialOpts::peer_id(peer)
                    .addresses(vec![relay_address
                        .clone()
                        .with(Protocol::P2pCircuit)
                        .with(Protocol::P2p(PeerId::from(peer).into()))])
                    .extend_addresses_through_behaviour()
                    .build();

                // establish relay-connection with remote peer
                swarm.dial(opts).unwrap();
            }
        }
    }

    Ok(swarm)
}

/// Network service that configures and deploys a libp2p network swarm over QUIC transports.
///
/// The swarm listens for incoming connections, dials remote nodes, manages connections and
/// executes predefined network behaviours.
pub async fn relay(
    network_config: &NetworkConfiguration,
    key_pair: libp2p::identity::Keypair,
) -> Result<Swarm<RelayBehaviour>> {
    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_relay_swarm(&network_config, key_pair).await?;

    // Listen on all interfaces
    let listen_addr_tcp = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Tcp(2030));
    swarm.listen_on(listen_addr_tcp)?;

    let listen_addr_quic = Multiaddr::empty()
        .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(network_config.quic_port))
        .with(Protocol::QuicV1);
    swarm.listen_on(listen_addr_quic)?;

    loop {
        match swarm.next().await.expect("Infinite Stream.") {
            SwarmEvent::Behaviour(event) => {
                if let RelayBehaviourEvent::Identify(identify::Event::Received {
                    info: identify::Info { observed_addr, .. },
                    ..
                }) = &event
                {
                    swarm.add_external_address(observed_addr.clone());
                }

                info!("{event:?}");
                break;
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {address:?}");
            }
            _ => {}
        }
    }

    info!("Relay initialized");

    Ok(swarm)
}

/// Main loop polling the async swarm event stream and incoming service messages stream.
struct EventLoop<T: NetworkBehaviour>
where
    T: NetworkBehaviour,
    <T as NetworkBehaviour>::ToSwarm: std::fmt::Debug,
{
    swarm: Swarm<T>,
    tx: ServiceSender,
    rx: BroadcastStream<ServiceMessage>,
    network_config: NetworkConfiguration,
    shutdown_handler: ShutdownHandler,
}

impl<T> EventLoop<T>
where
    T: NetworkBehaviour,
    <T as NetworkBehaviour>::ToSwarm: std::fmt::Debug,
{
    pub fn new(
        swarm: Swarm<T>,
        tx: ServiceSender,
        network_config: NetworkConfiguration,
        shutdown_handler: ShutdownHandler,
    ) -> Self {
        Self {
            swarm,
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
                    let event = event.unwrap();
                    info!("{event:?}");
                }
                event = self.rx.next() => match event {
                    Some(Ok(message)) => (),
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
}
