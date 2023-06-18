// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::operation::OperationId;

use crate::manager::Sender;
use crate::network::Peer;
use crate::replication::SyncMessage;

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Messages which can be sent on the communication bus.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMessage {
    /// A new operation arrived at the node.
    NewOperation(OperationId),

    /// Node established a bi-directional connection to another node.
    PeerConnected(Peer),

    /// Node closed a connection to another node.
    PeerDisconnected(Peer),

    /// Node sent a message to remote node for replication.
    SentReplicationMessage(Peer, SyncMessage),

    /// Node received a message from remote node for replication.
    ReceivedReplicationMessage(Peer, SyncMessage),

    /// Replication protocol failed with an critical error.
    ReplicationFailed(Peer),
}
