// SPDX-License-Identifier: AGPL-3.0-or-later

use std::cmp::Ordering;

use libp2p::swarm::ConnectionId;
use libp2p::PeerId;
use p2panda_rs::Human;

/// Identifier of a p2panda peer.
///
/// Additional to the unique `PeerId` we also store the `ConnectionId` to understand which libp2p
/// connection handler deals with the communication with that peer. In case connections get stale
/// or fail we can use this information to understand which peer got affected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Peer(PeerId, ConnectionId);

impl Peer {
    /// Returns a new instance of a peer.
    pub fn new(peer_id: PeerId, connection_id: ConnectionId) -> Self {
        Self(peer_id, connection_id)
    }

    /// Returns a new instance of our local peer.
    ///
    /// Local peers can not have a connection "to themselves", still we want to be able to compare
    /// our local peer with a remote one. This method therefore sets a "fake" `ConnectionId`.
    pub fn new_local_peer(local_peer_id: PeerId) -> Self {
        Self(local_peer_id, ConnectionId::new_unchecked(0))
    }

    /// Returns the `PeerId` of this peer.
    ///
    /// The `PeerId` is used to determine which peer "wins" over a duplicate session conflict.
    pub fn id(&self) -> PeerId {
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
        // only look at the internal `PeerId` since this is what both peers (local and remote)
        // know about (the connection id might be different)
        self.0.cmp(&other.0)
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
        let connection_id = format!("{:?}", self.1);
        let connection_id = connection_id[13..]
            .strip_suffix(')')
            .expect("ConnectionId format is as expected");
        format!("{} ({})", self.0, connection_id)
    }
}

#[cfg(test)]
mod tests {
    use libp2p::identity::Keypair;
    use libp2p::swarm::ConnectionId;
    use p2panda_rs::Human;

    use super::Peer;

    #[test]
    fn peers_display() {
        let peer_id = Keypair::generate_ed25519().public().to_peer_id();
        let peer_connection_2 = Peer::new(peer_id, ConnectionId::new_unchecked(2));
        assert_eq!(peer_connection_2.display(), format!("{} ({})", peer_id, 2));

        let peer_connection_23 = Peer::new(peer_id, ConnectionId::new_unchecked(23));
        assert_eq!(
            peer_connection_23.display(),
            format!("{} ({})", peer_id, 23)
        );

        let peer_connection_999 = Peer::new(peer_id, ConnectionId::new_unchecked(999));
        assert_eq!(
            peer_connection_999.display(),
            format!("{} ({})", peer_id, 999)
        );
    }
}
