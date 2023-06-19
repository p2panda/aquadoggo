// SPDX-License-Identifier: AGPL-3.0-or-later

use std::cmp::Ordering;

use libp2p::swarm::ConnectionId;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::Human;

/// Identifier of a p2panda peer.
///
/// Additional to the unique public key we also store the `ConnectionId` to understand which libp2p
/// connection handler deals with the communication with that peer. In case connections get stale
/// or fail we can use this information to understand which peer got affected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Peer(PublicKey, ConnectionId);

impl Peer {
    /// Returns a new instance of a peer.
    pub fn new(public_key: PublicKey, connection_id: ConnectionId) -> Self {
        Self(public_key, connection_id)
    }

    /// Returns a new instance of our local peer.
    ///
    /// Local peers can not have a connection "to themselves", still we want to be able to compare
    /// our local peer with a remote one. This method therefore sets a "fake" `ConnectionId`.
    pub fn new_local_peer(local_public_key: PublicKey) -> Self {
        Self(local_public_key, ConnectionId::new_unchecked(0))
    }

    /// Returns the public key of this peer which serves as its identifier.
    ///
    /// The public key is used to determine which peer "wins" over a duplicate session conflict.
    pub fn id(&self) -> PublicKey {
        self.0
    }

    /// Returns the `ConnectionId` which handles the bi-directional communication to that peer.
    pub fn connection_id(&self) -> ConnectionId {
        self.1
    }
}

impl Ord for Peer {
    fn cmp(&self, other: &Self) -> Ordering {
        // When comparing `Peer` instances (for example to handle duplicate session requests), we
        // only look at the internal `PublicKey` since this is what both peers (local and remote)
        // know about (the connection id might be different)
        self.0.to_bytes().cmp(&other.0.to_bytes())
    }
}

impl PartialOrd for Peer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Human for Peer {
    fn display(&self) -> String {
        // Trick to nicely display `ConnectionId` struct
        let connection_id = &format!("{:?}", self.1)[13..][..1];
        format!("{} ({})", self.0, connection_id)
    }
}