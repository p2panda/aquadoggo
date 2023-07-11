// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::dummy::ConnectionHandler as DummyConnectionHandler;
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour,
    PollParameters, SwarmEvent, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{warn, debug};

/// The empty type for cases which can't occur.
#[derive(Clone, Debug)]
pub enum Event {
    DialPeer(PeerId),
}

#[derive(Clone, Debug, Default)]
pub struct PeerStatus {
    connections: i32,
    dial_attempts: i32,
}

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<PeerId>,
    peers: HashMap<PeerId, PeerStatus>,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            peers: HashMap::new(),
        }
    }

    pub fn peer_discovered(&mut self, peer_id: PeerId) {
        if self.peers.get(&peer_id).is_none() {
            self.peers.insert(peer_id, PeerStatus::default());
            self.events
                .push_back(peer_id.clone());
        }
    }

    pub fn peer_expired(&mut self, peer_id: PeerId) {
        self.remove_peer(&peer_id)
    }

    fn redial_peer(&mut self, peer_id: &PeerId) {
        debug!("Re-dial peer: {peer_id:?}");
        if let Some(status) = self.peers.get(peer_id) {
            if status.connections == 0 {
                if status.dial_attempts < 10 {
                    self.events
                        .push_back(peer_id.clone());
                } else {
                    debug!("Re-dial attempt limit reached: remove peer {peer_id:?}");
                    self.remove_peer(&peer_id)
                }
            }    
        }
    }

    fn on_connection_established(&mut self, peer_id: PeerId) {
        match self.peers.get_mut(&peer_id) {
            Some(status) => {
                status.dial_attempts = 0;
                status.connections += 1;
            }
            None => {
                warn!("Connected established to unknown peer");
            }
        }
    }

    fn on_connection_closed(&mut self, peer_id: &PeerId) {
        match self.peers.get_mut(peer_id) {
            Some(status) => {
                status.connections -= 1;

                self.redial_peer(peer_id)

            }
            None => {
                warn!("Connected closed to unknown peer");
            }
        }
    }

    fn on_dial_failed(&mut self, peer_id: &PeerId) {
        match self.peers.get_mut(peer_id) {
            Some(status) => {
                status.dial_attempts += 1;

                self.redial_peer(peer_id)
            }
            None => warn!("Dial failed to unknown peer"),
        }
    }

    fn remove_peer(&mut self, peer_id: &PeerId) {
        if self.peers.remove(&peer_id).is_none() {
            warn!("Tried to remove unknown peer")
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = DummyConnectionHandler;

    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(DummyConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(DummyConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished { peer_id, .. }) => {
                self.on_connection_established(peer_id);
            }
            FromSwarm::ConnectionClosed(ConnectionClosed { peer_id, .. }) => {
                self.on_connection_closed(&peer_id);
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id,
                error,
                connection_id,
            }) => {
                if let Some(peer_id) = peer_id {
                    self.on_dial_failed(&peer_id);
                } else {
                    warn!("Dial failed to unknown peer")
                }
            }
            FromSwarm::AddressChange(_)
            | FromSwarm::ListenFailure(_)
            | FromSwarm::NewListener(_)
            | FromSwarm::NewListenAddr(_)
            | FromSwarm::ExpiredListenAddr(_)
            | FromSwarm::ListenerError(_)
            | FromSwarm::ListenerClosed(_)
            | FromSwarm::NewExternalAddrCandidate(_)
            | FromSwarm::ExternalAddrConfirmed(_)
            | FromSwarm::ExternalAddrExpired(_) => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _id: PeerId,
        _: ConnectionId,
        _: THandlerOutEvent<Self>,
    ) {
        ()
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(peer_id) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer_id).build(),
            });
            // return Poll::Ready(event);
        }

        Poll::Pending
    }
}
