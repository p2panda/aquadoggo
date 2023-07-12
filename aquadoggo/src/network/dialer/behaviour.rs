// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dummy::ConnectionHandler as DummyConnectionHandler;
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour,
    PollParameters, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{debug, warn};
use void::Void;

const DIAL_LIMIT: i32 = 10;

/// Events which are sent to the swarm from the dialer.
#[derive(Clone, Debug)]
pub enum Event {
    Dial(PeerId),
}

/// Status of a peer known to the dealer behaviour.
#[derive(Clone, Debug, Default)]
pub struct PeerStatus {
    /// Number of existing connections to this peer.
    connections: i32,

    /// Number of failed dial attempts since the last successful connection.
    ///
    /// Resets back to zero once a connection is established.
    dial_attempts: i32,
}

/// Behaviour responsible for dialing peers discovered by the `swarm`. If all connections close to
/// a peer, then the dialer attempts to reconnect to the peer.
///
/// @TODO: Need to add back-off to redial attempts.
#[derive(Debug)]
pub struct Behaviour {
    /// The behaviours event queue which gets consumed when `poll` is called.
    events: VecDeque<ToSwarm<Event, Void>>,

    /// Peers we are dialing.
    peers: HashMap<PeerId, PeerStatus>,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            peers: HashMap::new(),
        }
    }

    /// Add a peer to the map of known peers which this behaviour should be dialing.
    pub fn peer_discovered(&mut self, peer_id: PeerId) {
        if self.peers.get(&peer_id).is_none() {
            self.peers.insert(peer_id, PeerStatus::default());
            self.events
                .push_back(ToSwarm::GenerateEvent(Event::Dial(peer_id.clone())));
        }
    }

    /// Remove a peer from the behaviours map of known peers.
    ///
    /// Called externally if a peer is known to have expired.
    pub fn peer_expired(&mut self, peer_id: PeerId) {
        self.remove_peer(&peer_id)
    }

    /// Issues `Dial` event to the `swarm` for the passed peer.
    ///
    /// Checks `dial_attempts` for peer first, if this has exceeded the `DIAL_LIMIT` then no
    /// `Dial` event is issued.
    fn dial_peer(&mut self, peer_id: &PeerId) {
        if let Some(status) = self.peers.get_mut(peer_id) {
            status.dial_attempts += 1;
            if status.dial_attempts < DIAL_LIMIT {
                debug!(
                    "Attempt redial {} with peer: {peer_id:?}",
                    status.dial_attempts
                );
                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::Dial(peer_id.clone())));
            } else {
                debug!("Re-dial attempt limit reached: remove peer {peer_id:?}");
                self.remove_peer(&peer_id)
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

                if status.connections == 0 {
                    self.dial_peer(peer_id)
                }
            }
            None => {
                warn!("Connected closed to unknown peer");
            }
        }
    }

    fn on_dial_failed(&mut self, peer_id: &PeerId) {
        match self.peers.get_mut(peer_id) {
            Some(status) => {
                if status.connections == 0 {
                    self.dial_peer(peer_id)
                }
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
            FromSwarm::DialFailure(DialFailure { peer_id, .. }) => {
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
        ();
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}
