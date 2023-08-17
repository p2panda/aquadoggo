// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use exponential_backoff::Backoff;
use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::dummy::ConnectionHandler as DummyConnectionHandler;
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, PollParameters,
    THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{debug, warn};

const RETRY_LIMIT: u32 = 8;
const BACKOFF_SEC_MIN: u64 = 1;
const BACKOFF_SEC_MAX: u64 = 60;

/// Events which are sent to the swarm from the dialer.
#[derive(Clone, Debug)]
pub enum Event {
    /// Event sent to request that the swarm dials a known peer.
    Dial(PeerId, Multiaddr),
}

/// Status of a peer known to the dialer behaviour.
#[derive(Clone, Debug)]
pub struct RetryStatus {
    /// Number of dial attempts to this peer since it was last disconnected.
    ///
    /// Is reset to 0 once a connection is successfully established.
    attempts: u32,

    /// Time at which the peer should be dialed. Is None if this peer has a successfully
    /// established connection.
    next_dial: Option<Instant>,
}

/// Behaviour responsible for dialing peers discovered by the `swarm` and retrying when dial
/// attempts fail or a connection is unexpectedly closed.
///
/// Maintains two lists of peers to be dialed:
/// 1) A queue of peers we want to make a new dial attempt to are queued up in `dial`. Peers are
///    placed here when they are first discovered or when an existing connection unexpectedly closes.
/// 2) Peers whose's initial dial failed are held in `retry` and dial attempts are retried with a
///    backoff until `RETRY_LIMIT` is reached. Peers are placed here when a `DialFailed` event is
///    issued from the swarm.
#[derive(Debug)]
pub struct Behaviour {
    /// Addresses of known peers.
    address_book: HashMap<PeerId, Multiaddr>,

    /// Map of peers whose initial dial attempt failed and we want to re-dial.
    scheduled_dials: HashMap<PeerId, RetryStatus>,

    /// The backoff instance used for scheduling next_dials.
    backoff: Backoff,
}

impl Behaviour {
    pub fn new() -> Self {
        let min = Duration::from_secs(BACKOFF_SEC_MIN);
        let max = Duration::from_secs(BACKOFF_SEC_MAX);
        let mut backoff = Backoff::new(RETRY_LIMIT, min, max);
        backoff.set_jitter(0.9);

        Self {
            scheduled_dials: HashMap::new(),
            address_book: HashMap::new(),
            backoff,
        }
    }

    /// Add a peer to the address book.
    pub fn add_peer(&mut self, peer_id: PeerId, address: Multiaddr) {
        self.address_book.insert(peer_id, address);
    }

    /// Push a known peer to the dial queue.
    pub fn dial_peer(&mut self, peer_id: PeerId) {
        if !self.scheduled_dials.contains_key(&peer_id) {
            self.schedule_dial(&peer_id);
        }
    }

    /// Inform the behaviour that an unexpected error occurred on a connection to a peer, passing
    /// in the number of remaining connections which exist.
    ///
    /// If there are no remaining connections for this peer we add the peer to the dial queue.
    pub fn connection_error(&mut self, peer_id: PeerId, remaining_connections: u32) {
        if remaining_connections == 0 {
            self.dial_peer(peer_id);
        }
    }

    /// Schedule a peer to be re-dialed.
    ///
    /// Uses the configured `backoff` instance for the behaviour and the current `attempts` count
    /// for the passed node to set the `next_dial` time for this peer.
    ///
    /// If the `REDIAL_LIMIT` is reached then the peer is removed from the map of known peers and
    /// no more redial attempts will occur.
    fn schedule_dial(&mut self, peer_id: &PeerId) {
        if let Some(status) = self.scheduled_dials.get_mut(peer_id) {
            // If the peer is already in the `scheduled_dials` map then we check if there is a next backoff
            // delay based on the current number of retry attempts.
            if let Some(backoff_delay) = self.backoff.next(status.attempts) {
                // If we haven't reached `RETRY_LIMIT` then we set the `next_dial` time.
                status.next_dial = Some(Instant::now() + backoff_delay);
            } else {
                // If we have reached `RETRY_LIMIT` then we remove the peer from the `scheduled_dials` map.
                debug!("Re-dial attempt limit reached: {peer_id:?}");
                self.scheduled_dials.remove(peer_id);
            }
        } else {
            // If this peer was not in the `scheduled_dials` map yet we instantiate a new retry status and
            // insert the peer.
            let backoff_delay = self
                .backoff
                .next(1)
                .expect("Retry limit should be greater than 1");

            let status = RetryStatus {
                attempts: 1,
                next_dial: Some(Instant::now() + backoff_delay),
            };
            self.scheduled_dials.insert(peer_id.to_owned(), status);
        }
    }

    /// Inform the behaviour that a connection to a peer was established.
    ///
    /// When a connection was successfully established we remove the peer from the retry map as we
    /// no longer need to re-dial them.
    fn on_connection_established(&mut self, peer_id: &PeerId) {
        if self.scheduled_dials.remove(peer_id).is_some() {
            debug!("Removed peer from retry queue: {peer_id}");
        };
    }

    /// Inform the behaviour that a dialing attempt failed.
    ///
    /// We want the schedule the next dialing attempt or drop the peer completely if we have
    /// reached `RETRY_LIMIT`.
    fn on_dial_failed(&mut self, peer_id: &PeerId) {
        self.schedule_dial(peer_id)
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
                self.on_connection_established(&peer_id)
            }
            FromSwarm::DialFailure(DialFailure { peer_id, error, .. }) => match error {
                libp2p::swarm::DialError::DialPeerConditionFalse(err) => {
                    debug!("Not dialing peer as dial condition not met: {peer_id:?} {err:#?}")
                }
                err => {
                    debug!("Unexpected error dialing peer: {peer_id:?} {err:?}");
                    if let Some(peer_id) = peer_id {
                        self.on_dial_failed(&peer_id);
                    }
                }
            },
            FromSwarm::ConnectionClosed(_)
            | FromSwarm::AddressChange(_)
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
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        let mut peer_to_retry = None;
        let now = Instant::now();

        // Iterate over all peers and take the first one which has `next_dial` set and the
        // scheduled dial time has passed.
        for (peer_id, status) in &self.scheduled_dials {
            if let Some(next_dial) = status.next_dial {
                if next_dial < now {
                    peer_to_retry = Some(peer_id.to_owned());
                    break;
                }
            }
        }

        if let Some(peer_id) = peer_to_retry {
            // Unwrap safely as we know the peer exists.
            let status = self.scheduled_dials.get_mut(&peer_id).unwrap();

            // Increment the peers dial attempts.
            status.attempts += 1;

            // Set the peers `next_dial` value to None. This get's set again if the dial attempt fails.
            status.next_dial = None;

            debug!("Dial attempt {} for peer {peer_id}", status.attempts);

            if let Some(addr) = self.address_book.get(&peer_id) {
                let opts = DialOpts::peer_id(peer_id.clone())
                    .addresses(vec![addr.to_owned()])
                    .condition(PeerCondition::NotDialing)
                    .condition(PeerCondition::Disconnected)
                    .build();

                return Poll::Ready(ToSwarm::Dial { opts });
            };
        }

        Poll::Pending
    }
}
