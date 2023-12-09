// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::operation::OperationId;
use tokio::sync::broadcast;

use crate::manager::Sender;
use crate::network::{Peer, PeerMessage};

/// Sender for cross-service communication bus.
pub type ServiceSender = Sender<ServiceMessage>;

/// Creates a broadcast channel for use as a service bus.
pub fn create_service_sender(capacity: usize) -> ServiceSender {
    let (tx, _) = broadcast::channel(capacity);
    tx
}

/// Messages which can be sent on the communication bus.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMessage {
    /// A new operation arrived at the node.
    NewOperation(OperationId),

    DocumentUpdated(DocumentId),

    /// Node established a bi-directional connection to another node.
    PeerConnected(Peer),

    /// Node closed a connection to another node.
    PeerDisconnected(Peer),

    /// Node sent a message to remote node.
    SentMessage(Peer, PeerMessage),

    /// Node received a message from remote node.
    ReceivedMessage(Peer, PeerMessage),

    /// Replication protocol failed with an critical error.
    ReplicationFailed(Peer),
}
