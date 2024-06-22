// SPDX-License-Identifier: AGPL-3.0-or-later

use libp2p::multiaddr::Protocol;
use libp2p::{rendezvous, Multiaddr, PeerId, Swarm};

use crate::network::behaviour::P2pandaBehaviour;
use crate::network::config::NODE_NAMESPACE;

/// A relay node.
pub struct Relay {
    /// PeerId of the relay node.
    pub(crate) peer_id: PeerId,

    /// A single Multiaddr which we know the relay node to be accessible at.
    pub(crate) addr: Multiaddr,

    /// The namespace we discover peers at on this relay.
    pub(crate) namespace: String,

    /// Did we tell the relay it's observed address yet.
    pub(crate) told_addr: bool,

    /// Are we currently discovering peers.
    pub(crate) discovering: bool,

    /// Are we in the process of registering at this relay.
    pub(crate) registering: bool,

    /// Have we successfully registered.
    pub(crate) registered: bool,

    /// Was our relay circuit reservation accepted.
    pub(crate) reservation_accepted: bool,
}

impl Relay {
    pub fn new(peer_id: PeerId, addr: Multiaddr) -> Self {
        Relay {
            peer_id,
            addr,
            namespace: NODE_NAMESPACE.to_string(),
            told_addr: false,
            discovering: false,
            registering: false,
            registered: false,
            reservation_accepted: false,
        }
    }

    /// The circuit address we should listen at for this relay.
    pub fn circuit_addr(&self) -> Multiaddr {
        self.addr
            .clone()
            .with(Protocol::P2p(self.peer_id))
            .with(Protocol::P2pCircuit)
    }

    /// Start listening on the relay circuit address and register on our discovery namespace.
    pub fn register(&mut self, swarm: &mut Swarm<P2pandaBehaviour>) -> Result<bool, anyhow::Error> {
        if self.registered || self.registering {
            return Ok(false);
        }

        self.registering = true;

        // Start listening on the circuit relay address.
        let circuit_address = self.circuit_addr();
        swarm.listen_on(circuit_address.clone())?;

        // Register in the `NODE_NAMESPACE` using the rendezvous network behaviour.
        swarm
            .behaviour_mut()
            .rendezvous_client
            .as_mut()
            .unwrap()
            .register(
                rendezvous::Namespace::from_static(NODE_NAMESPACE),
                self.peer_id.clone(),
                None, // Default ttl is 7200s
            )?;

        Ok(true)
    }

    /// Start discovering peers also registered at the same namespace.
    pub fn discover(&mut self, swarm: &mut Swarm<P2pandaBehaviour>) -> bool {
        if self.reservation_accepted && self.registered && !self.discovering {
            self.discovering = true;

            swarm
                .behaviour_mut()
                .rendezvous_client
                .as_mut()
                .expect("Relay client behaviour exists")
                .discover(
                    Some(
                        rendezvous::Namespace::new(NODE_NAMESPACE.to_string())
                            .expect("Valid namespace"),
                    ),
                    None,
                    None,
                    self.peer_id,
                );

            true
        } else {
            false
        }
    }
}
