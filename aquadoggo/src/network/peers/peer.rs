// SPDX-License-Identifier: AGPL-3.0-or-later

use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result};

use libp2p::swarm::ConnectionId;
use libp2p::PeerId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Peer(PeerId, ConnectionId);

impl Peer {
    pub fn new(peer_id: PeerId, connection_id: ConnectionId) -> Self {
        Self(peer_id, connection_id)
    }

    pub fn new_local_peer(local_peer_id: PeerId) -> Self {
        Self(local_peer_id, ConnectionId::new_unchecked(0))
    }

    pub fn id(&self) -> PeerId {
        self.0
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.1
    }
}

impl Ord for Peer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Peer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "Peer({}, {:?})", self.0, self.1)
    }
}
