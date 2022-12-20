// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures::StreamExt;
use libp2p::core::upgrade;
use libp2p::floodsub::{self, Floodsub, FloodsubEvent};
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{identity, mdns, mplex, noise, tcp, Multiaddr, PeerId, Transport};
use log::{debug, info, warn};
use std::time::Duration;
use tokio::task;

use crate::bus::ServiceSender;
use crate::context::Context;
use crate::manager::{ServiceReadySender, Shutdown};

pub async fn libp2p_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let mut _rx = tx.subscribe();

    // Create a random PeerId
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    info!("Local peer id: {peer_id:?}");

    // Create a tokio-based TCP transport use noise for authenticated
    // encryption and Mplex for multiplexing of substreams on a TCP stream.
    let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1)
        .authenticate(
            noise::NoiseAuthenticated::xx(&id_keys)
                .expect("Signing libp2p-noise static DH keypair failed."),
        )
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    // Create a Floodsub topic
    let floodsub_topic = floodsub::Topic::new("aquadoggo");

    // We create a custom  behaviour that combines floodsub and mDNS.
    // The derive generates a delegating `NetworkBehaviour` impl.
    #[derive(NetworkBehaviour)]
    #[behaviour(out_event = "MyBehaviourEvent")]
    struct MyBehaviour {
        floodsub: Floodsub,
        mdns: mdns::tokio::Behaviour,
    }

    #[allow(clippy::large_enum_variant)]
    enum MyBehaviourEvent {
        Floodsub(FloodsubEvent),
        Mdns(mdns::Event),
    }

    impl From<FloodsubEvent> for MyBehaviourEvent {
        fn from(event: FloodsubEvent) -> Self {
            MyBehaviourEvent::Floodsub(event)
        }
    }

    impl From<mdns::Event> for MyBehaviourEvent {
        fn from(event: mdns::Event) -> Self {
            MyBehaviourEvent::Mdns(event)
        }
    }

    // Create a Swarm to manage peers and events.
    let mdns_behaviour = mdns::Behaviour::new(Default::default())?;
    let mut behaviour = MyBehaviour {
        floodsub: Floodsub::new(peer_id),
        mdns: mdns_behaviour,
    };
    behaviour.floodsub.subscribe(floodsub_topic.clone());
    let mut swarm = Swarm::with_tokio_executor(transport, behaviour, peer_id);

    // Reach out to another node if specified
    if let Some(to_dial) = context.config.replication.remote_peers.get(0) {
        let addr: Multiaddr = to_dial.parse()?;
        swarm.dial(addr)?;
        info!("Dialed {to_dial:?}");
    }

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Setup a channel for saying hi!
    let (beep, mut boop) = tokio::sync::mpsc::channel(100);
    let _ = task::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = beep.send(String::from("Hi, I'm aquadoggo!")).await;
        }
    });

    // Kick it all off
    let handle = task::spawn(async move {
        loop {
            tokio::select! {
                Some(beep) = boop.recv() => {
                    swarm.behaviour_mut().floodsub.publish(floodsub_topic.clone(), beep.as_bytes());
                }
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("Listening on {address:?}");
                        }
                        SwarmEvent::Behaviour(MyBehaviourEvent::Floodsub(FloodsubEvent::Message(message))) => {
                            info!(
                                    "Received: '{:?}' from {:?}",
                                    String::from_utf8_lossy(&message.data),
                                    message.source
                                );
                        }
                        SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(event)) => {
                            match event {
                                mdns::Event::Discovered(list) => {
                                    for (peer, _) in list {
                                        swarm.behaviour_mut().floodsub.add_node_to_partial_view(peer);
                                    };
                                }
                                mdns::Event::Expired(list) => {
                                    for (peer, _) in list {
                                        if !swarm.behaviour().mdns.has_node(&peer) {
                                            swarm.behaviour_mut().floodsub.remove_node_from_partial_view(&peer);
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    debug!("libp2p service is ready");
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
