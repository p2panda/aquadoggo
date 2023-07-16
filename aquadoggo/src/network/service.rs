// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::Duration;

use anyhow::Result;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Event;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionError, NetworkBehaviour, SwarmEvent};
use libp2p::{autonat, identify, mdns, rendezvous, Multiaddr, PeerId, Swarm};
use log::{debug, info, trace, warn};
use tokio::task;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::behaviour::{ClientBehaviour, RelayBehaviour};
use crate::network::config::NODE_NAMESPACE;
use crate::network::{dialer, identity, peers, swarm, NetworkConfiguration, ShutdownHandler};

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

    // Build the network swarm and retrieve the local peer ID
    let mut swarm = swarm::build_client_swarm(&network_config, key_pair).await?;

    // Define the QUIC multiaddress on which the swarm will listen for connections
    let quic_multiaddr =
        format!("/ip4/0.0.0.0/udp/{}/quic-v1", network_config.quic_port).parse()?;

    // Listen for incoming connection requests over the QUIC transport
    swarm.listen_on(quic_multiaddr)?;

    let mut external_circuit_addr = None;

    // Construct circuit relay addresses and listen on relayed address
    if let Some(relay_addr) = &network_config.relay_address {
        let circuit_addr = relay_addr.clone().with(Protocol::P2pCircuit);

        // Dialable circuit relay address for local node
        external_circuit_addr = Some(circuit_addr.clone().with(Protocol::P2p(local_peer_id)));

        swarm.listen_on(circuit_addr)?;
    }

    // Dial each peer identified by the multi-address provided via `--remote-peers` if given
    for addr in network_config.remote_peers.clone() {
        swarm.dial(addr)?
    }

    // Dial the peer identified by the multi-address provided via `--rendezvous_address` if given
    if let Some(addr) = network_config.rendezvous_address.clone() {
        swarm.dial(addr)?;
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

    // Define the QUIC multiaddress on which the swarm will listen for connections
    let quic_multiaddr =
        format!("/ip4/0.0.0.0/udp/{}/quic-v1", network_config.quic_port).parse()?;

    // Listen for incoming connection requests over the QUIC transport
    swarm.listen_on(quic_multiaddr)?;

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
                    println!("{event:?}");
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
