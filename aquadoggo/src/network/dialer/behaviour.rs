// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::task::{Context, Poll};

use exponential_backoff::Backoff;
use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dummy::ConnectionHandler as DummyConnectionHandler;
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour,
    PollParameters, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{debug, warn};
use std::time::{Duration, Instant};

const RETRY_LIMIT: u32 = 8;
const BACKOFF_SEC_MIN: u64 = 1;
const BACKOFF_SEC_MAX: u64 = 10;

/// Events which are sent to the swarm from the dialer.
#[derive(Clone, Debug)]
pub enum Event {
    /// Event sent to request that the swarm dials a known peer.
    Dial(PeerId),
}

/// Status of a peer known to the dealer behaviour.
#[derive(Clone, Debug, Default)]
pub struct PeerStatus {
    /// Number of existing connections to this peer.
    connections: u32,

    /// Number of dial attempts to this peer since it was last disconnected.
    ///
    /// Is reset to 0 once a connection is successfully established.
    attempts: u32,

    /// Time at which the peer should be dialed. Is None if this peer has a successfully
    /// established connection.
    next_dial: Option<Instant>,
}

/// Behaviour responsible for dialing peers discovered by the `swarm`. If all connections close to
/// a peer, then the dialer attempts to reconnect to the peer.
///
/// @TODO: Need to add back-off to next_dial attempts.
#[derive(Debug)]
pub struct Behaviour {
    /// Peers we want to dial.
    peers: HashMap<PeerId, PeerStatus>,

    /// The backoff instance used for scheduling next_dials.
    backoff: Backoff,
}

impl Behaviour {
    pub fn new() -> Self {
        let min = Duration::from_secs(BACKOFF_SEC_MIN);
        let max = Duration::from_secs(BACKOFF_SEC_MAX);
        let backoff = Backoff::new(RETRY_LIMIT, min, max);

        Self {
            peers: HashMap::new(),
            backoff,
        }
    }

    /// Add a peer to the map of known peers who should be dialed.
    pub fn peer_discovered(&mut self, peer_id: PeerId) {
        if self.peers.get(&peer_id).is_none() {
            self.peers.insert(peer_id, PeerStatus::default());
            self.schedule_dial(&peer_id);
        }
    }

    /// Remove a peer from the map of known peers.
    ///
    /// Called externally if a peers registration expires on the `swarm`.
    pub fn peer_expired(&mut self, peer_id: PeerId) {
        // If the peer doesn't exist this means it was already dropped because the RETRY_LIMIT was
        // reached, so don't try and remove it again.
        if self.peers.get(&peer_id).is_some() {
            self.remove_peer(&peer_id)
        }
    }

    /// Schedule a peer to be dialed.
    ///
    /// Uses the configured `backoff` instance for the behaviour and the current `attempts` count
    /// for the passed node to set the `next_dial` time for this peer.
    ///
    /// If the `REDIAL_LIMIT` is reached then the peer is removed from the map of known peers and
    /// no more redial attempts will occur.
    fn schedule_dial(&mut self, peer_id: &PeerId) {
        if let Some(status) = self.peers.get_mut(peer_id) {
            if let Some(backoff_delay) = self.backoff.next(status.attempts) {
                status.next_dial = Some(Instant::now() + backoff_delay);
            } else {
                debug!("Re-dial attempt limit reached: remove peer {peer_id:?}");
                self.remove_peer(peer_id)
            }
        }
    }

    fn on_connection_established(&mut self, peer_id: PeerId) {
        match self.peers.get_mut(&peer_id) {
            Some(status) => {
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
                    self.schedule_dial(peer_id)
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
                    self.schedule_dial(peer_id)
                }
            }
            None => warn!("Dial failed to unknown peer"),
        }
    }

    fn remove_peer(&mut self, peer_id: &PeerId) {
        if self.peers.remove(peer_id).is_none() {
            // Don't warn here, peers can be removed twice if their registration on the swarm
            // expired at the same time as their RETRY_LIMIT was reached.
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
    ) {}

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        let mut peer_to_dial = None;
        let now = Instant::now();

        // Iterate over all peers and take the first one which has `next_dial` set and the
        // scheduled dial time has passed.
        for (peer_id, status) in &self.peers {
            if let Some(next_dial) = status.next_dial {
                if next_dial < now {
                    peer_to_dial = Some(peer_id.to_owned());
                }
            }
        }

        if let Some(peer_id) = peer_to_dial {
            // Unwrap safely as we know the peer exists.
            let status = self.peers.get_mut(&peer_id).unwrap();

            // Increment the peers dial attempts.
            status.attempts += 1;

            // Set the peers `next_dial` value to None. This get's set again if the dial attempt fails.
            status.next_dial = None;

            debug!("Dial attempt {} for peer {peer_id}", status.attempts);
            return Poll::Ready(ToSwarm::GenerateEvent(Event::Dial(peer_id.to_owned())));
        }

        Poll::Pending
    }
}
