// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use log::{debug, info, trace, warn};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::Human;
use tokio::task;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::SqlStore;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::Peer;
use crate::replication::errors::ReplicationError;
use crate::replication::{
    Mode, Session, SessionId, SyncIngest, SyncManager, SyncMessage, TargetSet,
};
use crate::schema::SchemaProvider;

/// Maximum of replication sessions per peer.
const MAX_SESSIONS_PER_PEER: usize = 3;

/// How often does the scheduler check for initiating replication sessions with peers.
const UPDATE_INTERVAL: Duration = Duration::from_secs(5);

pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let _rx = tx.subscribe();

    let manager = ConnectionManager::new(
        &context.schema_provider,
        &context.store,
        &tx,
        context.key_pair.public_key(),
    );
    let handle = task::spawn(manager.run());

    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about replication service being ready");
    };

    tokio::select! {
        _ = handle => (),
        _ = shutdown => (),
    }

    Ok(())
}

/// Statistics about successful and failed replication sessions for each connected peer.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerStatus {
    peer: Peer,
    successful_count: usize,
    failed_count: usize,
}

impl PeerStatus {
    pub fn new(peer: Peer) -> Self {
        Self {
            peer,
            successful_count: 0,
            failed_count: 0,
        }
    }
}

/// Coordinates peer connections and replication sessions.
///
/// This entails:
///
/// 1. Handles incoming replication- and peer connection messages from other services
/// 2. Maintains a list of currently connected p2panda peers
/// 3. Routes messages to the right replication session with help of the `SyncManager` and returns
///    responses to other services
/// 4. Schedules new replication sessions
/// 5. Handles replication errors and informs other services about them
struct ConnectionManager {
    /// List of peers the connection mananger knows about and are available for replication.
    peers: HashMap<Peer, PeerStatus>,

    /// Replication state manager, data ingest and message generator for handling all replication
    /// logic.
    sync_manager: SyncManager<Peer>,

    /// Async stream giving us a regular interval to initiate new replication sessions.
    scheduler: IntervalStream,

    /// Receiver for messages from other services, for example the networking layer.
    tx: ServiceSender,

    /// Sender for messages to other services.
    rx: BroadcastStream<ServiceMessage>,

    /// Provider to retreive our currently supported schema ids.
    schema_provider: SchemaProvider,
}

impl ConnectionManager {
    /// Returns a new instance of `ConnectionManager`.
    pub fn new(
        schema_provider: &SchemaProvider,
        store: &SqlStore,
        tx: &ServiceSender,
        local_public_key: PublicKey,
    ) -> Self {
        let local_peer = Peer::new_local_peer(local_public_key);
        let ingest = SyncIngest::new(schema_provider.clone(), tx.clone());
        let sync_manager = SyncManager::new(store.clone(), ingest, local_peer);
        let scheduler = IntervalStream::new(interval(UPDATE_INTERVAL));

        Self {
            peers: HashMap::new(),
            sync_manager,
            scheduler,
            tx: tx.clone(),
            rx: BroadcastStream::new(tx.subscribe()),
            schema_provider: schema_provider.clone(),
        }
    }

    /// Returns set of schema ids we are interested in and support on this node.
    async fn target_set(&self) -> TargetSet {
        let supported_schema_ids: Vec<SchemaId> = self
            .schema_provider
            .all()
            .await
            .iter()
            .map(|schema| schema.id().to_owned())
            .collect();
        TargetSet::new(&supported_schema_ids)
    }

    /// Register a new peer in the network.
    async fn on_connection_established(&mut self, peer: Peer) {
        info!("Connected to peer: {}", peer.display());

        match self.peers.get(&peer) {
            Some(_) => {
                warn!("Peer already known: {}", peer.display());
            }
            None => {
                self.peers.insert(peer, PeerStatus::new(peer));
                self.update_sessions().await;
            }
        }
    }

    /// Handle a peer disconnecting from the network.
    async fn on_connection_closed(&mut self, peer: Peer) {
        info!("Disconnected from peer: {}", peer.display());

        // Clear running replication sessions from sync manager
        self.sync_manager.remove_sessions(&peer);
        self.remove_connection(peer)
    }

    /// Remove a peer from the network.
    fn remove_connection(&mut self, peer: Peer) {
        match self.peers.remove(&peer) {
            Some(_) => debug!("Remove peer: {}", peer.display()),
            None => warn!("Tried to remove connection from unknown peer"),
        }
    }

    /// Route incoming replication messages to the right session.
    async fn on_replication_message(&mut self, peer: Peer, message: SyncMessage) {
        let session_id = message.session_id();

        match self.sync_manager.handle_message(&peer, &message).await {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer, message,
                    ));
                }

                if result.is_done {
                    self.on_replication_finished(peer, session_id).await;
                }
            }
            Err(err) => {
                self.on_replication_error(peer, session_id, err).await;
            }
        }
    }

    /// Handle successful replication sessions.
    async fn on_replication_finished(&mut self, peer: Peer, _session_id: SessionId) {
        info!("Finished replication with peer {}", peer.display());

        match self.peers.get_mut(&peer) {
            Some(status) => {
                status.successful_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }
    }

    /// Handle replication errors and inform other services about them.
    async fn on_replication_error(
        &mut self,
        peer: Peer,
        session_id: SessionId,
        error: ReplicationError,
    ) {
        warn!("Replication with peer {} failed: {}", peer.display(), error);

        match self.peers.get_mut(&peer) {
            Some(status) => {
                status.failed_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }

        self.sync_manager.remove_session(&peer, &session_id);

        // Inform network service about error, so it can accordingly react
        self.send_service_message(ServiceMessage::ReplicationFailed(peer));
    }

    /// Determine if we can attempt new replication sessions with the peers we currently know
    /// about.
    async fn update_sessions(&mut self) {
        // Determine the target set our node is interested in
        let target_set = self.target_set().await;

        // Iterate through all currently connected peers
        let attempt_peers: Vec<Peer> = self
            .peers
            .clone()
            .into_iter()
            .filter_map(|(peer, _)| {
                let sessions = self.sync_manager.get_sessions(&peer);

                // 1. Check if we're running too many sessions with that peer on this connection
                //    already. This limit is configurable.
                let active_sessions: Vec<&Session> = sessions
                    .iter()
                    .filter(|session| !session.is_done())
                    .collect();

                // 2. Check if we're already having at least one session concerning the same target
                //    set. If we would start that session again it would be considered an error.
                let has_active_target_set_session = active_sessions
                    .iter()
                    .any(|session| session.target_set() == target_set);

                if active_sessions.len() < MAX_SESSIONS_PER_PEER && !has_active_target_set_session {
                    Some(peer)
                } else {
                    None
                }
            })
            .collect();

        if attempt_peers.is_empty() {
            trace!("No peers available for replication")
        }

        for peer in attempt_peers {
            self.initiate_replication(&peer, &target_set).await;
        }
    }

    /// Initiate a new replication session with remote peer.
    async fn initiate_replication(&mut self, peer: &Peer, target_set: &TargetSet) {
        match self
            .sync_manager
            .initiate_session(peer, target_set, &Mode::LogHeight)
            .await
        {
            Ok(messages) => {
                for message in messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        *peer, message,
                    ));
                }
            }
            Err(err) => {
                warn!("Replication error: {}", err)
            }
        }
    }

    /// Handles incoming messages from other services via the bus.
    async fn handle_service_message(&mut self, message: ServiceMessage) {
        match message {
            ServiceMessage::PeerConnected(peer) => {
                self.on_connection_established(peer).await;
            }
            ServiceMessage::PeerDisconnected(peer) => {
                self.on_connection_closed(peer).await;
            }
            ServiceMessage::ReceivedReplicationMessage(peer, message) => {
                self.on_replication_message(peer, message).await;
            }
            _ => (), // Ignore all other messages
        }
    }

    /// Sends a message on the bus to other services.
    fn send_service_message(&self, message: ServiceMessage) {
        if self.tx.send(message).is_err() {
            // Silently fail here as we don't care if the message was received at this
            // point
        }
    }

    /// Main event loop running the async streams.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.rx.next() => match event {
                    Some(Ok(message)) => self.handle_service_message(message).await,
                    Some(Err(err)) => {
                        panic!("Service bus subscriber for connection manager loop failed: {}", err);
                    }
                    // Command channel closed, thus shutting down the network event loop
                    None => {
                        return
                    },
                },
                Some(_) = self.scheduler.next() => {
                    self.update_sessions().await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use libp2p::swarm::ConnectionId;
    use libp2p::PeerId;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::network::Peer;
    use crate::replication::{Message, Mode, SyncMessage};
    use crate::test_utils::{test_runner, TestNode};

    use super::ConnectionManager;

    #[test]
    fn peer_lifetime() {
        let local_peer_id =
            PeerId::from_str("12D3KooWD3JAiSNrVGxjC7vJCcjwS8egbtJV9kzrstxLRKiwb9UY").unwrap();
        let remote_peer_id =
            PeerId::from_str("12D3KooWCqtLMJQLY3sm9rpDampJ2nPLswPPZto3mrRY7794QATF").unwrap();

        test_runner(move |node: TestNode| async move {
            let (tx, mut rx) = broadcast::channel::<ServiceMessage>(10);

            let mut manager = ConnectionManager::new(
                &node.context.schema_provider,
                &node.context.store,
                &tx,
                local_peer_id,
            );

            let target_set = manager.target_set().await;

            // Inform connection manager about new peer
            let remote_peer = Peer::new(remote_peer_id, ConnectionId::new_unchecked(1));

            manager
                .handle_service_message(ServiceMessage::PeerConnected(remote_peer))
                .await;

            let status = manager
                .peers
                .get(&remote_peer)
                .expect("Peer to be registered in connection manager");
            assert_eq!(manager.peers.len(), 1);
            assert_eq!(status.peer, remote_peer);

            // Manager attempts a replication session with that peer
            assert_eq!(rx.len(), 1);
            assert_eq!(
                rx.recv().await,
                Ok(ServiceMessage::SentReplicationMessage(
                    remote_peer,
                    SyncMessage::new(0, Message::SyncRequest(Mode::LogHeight, target_set))
                ))
            );
            assert_eq!(manager.sync_manager.get_sessions(&remote_peer).len(), 1);

            // Inform manager about peer disconnected
            manager
                .handle_service_message(ServiceMessage::PeerDisconnected(remote_peer))
                .await;

            // Manager cleans up internal state
            assert_eq!(rx.len(), 0);
            assert_eq!(manager.peers.len(), 0);
            assert_eq!(manager.sync_manager.get_sessions(&remote_peer).len(), 0);
        });
    }
}
