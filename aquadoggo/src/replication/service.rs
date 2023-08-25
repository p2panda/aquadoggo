// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use libp2p::PeerId;
use log::{debug, info, trace, warn};
use p2panda_rs::Human;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::task;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::SqlStore;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::network::identity::to_libp2p_peer_id;
use crate::network::{Peer, PeerMessage};
use crate::replication::errors::ReplicationError;
use crate::replication::{
    now, Announcement, AnnouncementMessage, Message, Mode, Session, SessionId, SyncIngest,
    SyncManager, SyncMessage, TargetSet,
};
use crate::schema::SchemaProvider;

/// Maximum number of peers we replicate with at one a time.
const MAX_PEER_SAMPLE: usize = 3;

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
        to_libp2p_peer_id(&context.key_pair.public_key()),
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
    /// Connectivity information like libp2p peer id and connection id.
    peer: Peer,

    /// Last known announcement of this peer.
    announcement: Option<Announcement>,

    /// Last time we've announced our local target set with this peer. Helps to check if we need to
    /// inform them about any updates from our side.
    sent_our_announcement_timestamp: u64,

    /// Number of successful replication sessions.
    successful_count: usize,

    /// Number of failed replication sessions.
    failed_count: usize,
}

impl PeerStatus {
    pub fn new(peer: Peer) -> Self {
        Self {
            peer,
            announcement: None,
            sent_our_announcement_timestamp: 0,
            successful_count: 0,
            failed_count: 0,
        }
    }
}

/// Coordinates peer connections and replication sessions.
///
/// This entails:
///
/// 1. Manages announcements of us and other peers about which schema ids are supported
/// 2. Handles incoming replication- and peer connection messages from other services
/// 3. Maintains a list of currently connected p2panda peers.
/// 4. Routes messages to the right replication session with help of the `SyncManager` and returns
///    responses to other services
/// 5. Schedules new replication sessions
/// 6. Handles replication errors and informs other services about them
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

    /// Provider to retrieve our currently supported schema ids.
    schema_provider: SchemaProvider,

    /// Our latest announcement state we want to propagate to all current and future peers. It
    /// contains a list of schema ids we're supporting as a node.
    announcement: Option<Announcement>,
}

impl ConnectionManager {
    /// Returns a new instance of `ConnectionManager`.
    pub fn new(
        schema_provider: &SchemaProvider,
        store: &SqlStore,
        tx: &ServiceSender,
        local_peer_id: PeerId,
    ) -> Self {
        let local_peer = Peer::new_local_peer(local_peer_id);
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
            announcement: None,
        }
    }

    /// Returns set of schema ids we are interested in and support on this node.
    async fn target_set(&self) -> TargetSet {
        let supported_schema_ids = self.schema_provider.supported_schema_ids().await;
        TargetSet::new(&supported_schema_ids)
    }

    /// Register a new peer connection on the manager.
    async fn on_connection_established(&mut self, peer: Peer) {
        info!("Established connection with peer: {}", peer.display());

        match self.peers.get(&peer) {
            Some(_) => {
                warn!("Peer already known: {}", peer.display());
            }
            None => {
                self.peers.insert(peer, PeerStatus::new(peer));
                self.on_update().await;
            }
        }
    }

    /// Routines which get executed on every scheduler beat and newly established connection.
    async fn on_update(&mut self) {
        // Inform new peers about our supported protocol version and schema ids
        self.announce().await;

        // Check if we can establish replication sessions with peers
        self.update_sessions().await;
    }

    /// Handle a peer connection closing.
    async fn on_connection_closed(&mut self, peer: Peer) {
        info!("Closed connection with peer: {}", peer.display());

        // Clear running replication sessions from sync manager
        self.sync_manager.remove_sessions(&peer);
        self.remove_connection(peer)
    }

    /// Remove a peer connection from the manager.
    fn remove_connection(&mut self, peer: Peer) {
        if self.peers.remove(&peer).is_none() {
            warn!("Tried to remove connection from unknown peer")
        }
    }

    /// Update announcement state of a remote peer.
    async fn on_announcement_message(&mut self, peer: Peer, message: AnnouncementMessage) {
        // Check if this node supports our replication protocol version
        if !message.is_version_supported() {
            return;
        }

        let incoming_announcement = message.announcement();

        match self.peers.get_mut(&peer) {
            Some(status) => match &status.announcement {
                Some(current) => {
                    // Only update peer status when incoming announcement has a newer timestamp
                    if current.timestamp < incoming_announcement.timestamp {
                        trace!(
                            "Received updated announcement state from peer {}",
                            peer.display()
                        );
                        status.announcement = Some(incoming_announcement);
                    }
                }
                None => {
                    trace!(
                        "Received first announcement state from peer {}",
                        peer.display()
                    );
                    status.announcement = Some(incoming_announcement);
                }
            },
            None => {
                trace!("Tried to update announcement state of unknown peer");
            }
        }
    }

    /// Route incoming replication messages to the right session.
    async fn on_replication_message(&mut self, peer: Peer, message: SyncMessage) {
        let session_id = message.session_id();

        // If this is a SyncRequest message first we check if the contained target set matches our
        // own locally configured one.
        if let Message::SyncRequest(_, remote_supported_schema_ids) = message.message() {
            let local_supported_schema_ids = &self
                .announcement
                .as_ref()
                .expect("Announcement state needs to be set with 'update_announcement'")
                .supported_schema_ids;

            // If this node has been configured with a whitelist of schema ids then we check the
            // target set of the requests matches our own, otherwise we skip this step and accept
            // any target set.
            if self.schema_provider.is_whitelist_active()
                && !local_supported_schema_ids.is_valid_set(remote_supported_schema_ids)
            {
                // If it doesn't match we signal that an error occurred and return at this point.
                self.on_replication_error(peer, session_id, ReplicationError::UnsupportedTargetSet)
                    .await;

                return;
            }
        }

        match self.sync_manager.handle_message(&peer, &message).await {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentMessage(
                        peer,
                        PeerMessage::SyncMessage(message),
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
        if let ReplicationError::NoSessionFound(_, _) = error {
            debug!("Replication session not found: {}", error);
        } else {
            warn!("Replication failed: {}", error);
        }

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

    /// Generates our new announcement state we can then propagate to all known and future peers.
    async fn update_announcement(&mut self) {
        let target_set = self.target_set().await;
        self.announcement = Some(Announcement::new(target_set));
    }

    /// Determine if we can attempt new replication sessions with the peers we currently know
    /// about.
    async fn update_sessions(&mut self) {
        let local_supported_schema_ids = &self
            .announcement
            .as_ref()
            .expect("Announcement state needs to be set with 'update_announcement'")
            .supported_schema_ids;

        // Iterate through all currently connected peers
        let mut attempt_peers: Vec<(Peer, TargetSet)> = self
            .peers
            .clone()
            .into_iter()
            .filter_map(|(peer, status)| {
                let sessions = self.sync_manager.get_sessions(&peer);

                // 1. Did we already receive this peers announcement state? If not we can't do
                //    anything yet and need to wait.
                let remote_supported_schema_ids: TargetSet =
                    if let Some(announcement) = status.announcement {
                        announcement.supported_schema_ids
                    } else {
                        return None;
                    };

                // 2. Calculate intersection of local and remote schema id sets. Do we have any
                //    supported schema id's in common?
                let target_set = TargetSet::from_intersection(
                    local_supported_schema_ids,
                    &remote_supported_schema_ids,
                );
                if target_set.is_empty() {
                    return None;
                }

                // 3. Check if we're running too many sessions with that peer on this connection
                //    already. This limit is configurable.
                let active_sessions: Vec<&Session> = sessions
                    .iter()
                    .filter(|session| !session.is_done())
                    .collect();

                // 4. Check if we're already having at least one session concerning the same target
                //    set. If we would start that session again it would be considered an error.
                let has_active_target_set_session = active_sessions
                    .iter()
                    .any(|session| session.target_set() == target_set);

                if active_sessions.len() < MAX_SESSIONS_PER_PEER && !has_active_target_set_session {
                    Some((peer, target_set))
                } else {
                    None
                }
            })
            .collect();

        if attempt_peers.is_empty() {
            trace!("No peers available for replication")
        }

        // Take a random sample of the remaining peers up to MAX_PEER_SAMPLE
        attempt_peers.shuffle(&mut thread_rng());
        attempt_peers.truncate(MAX_PEER_SAMPLE);

        for (peer, target_set) in &attempt_peers {
            self.initiate_replication(peer, target_set).await;
        }
    }

    /// Send our local announcement state to all peers which are not informed yet.
    async fn announce(&mut self) {
        let local_announcement = self
            .announcement
            .as_ref()
            .expect("Announcement state needs to be set with 'update_announcement'");

        for (peer, status) in &self.peers {
            if status.sent_our_announcement_timestamp < local_announcement.timestamp {
                self.send_service_message(ServiceMessage::SentMessage(
                    *peer,
                    PeerMessage::Announce(AnnouncementMessage::new(local_announcement.clone())),
                ));
            }
        }

        for (_, status) in self.peers.iter_mut() {
            if status.sent_our_announcement_timestamp < local_announcement.timestamp {
                status.sent_our_announcement_timestamp = now();
            }
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
                    self.send_service_message(ServiceMessage::SentMessage(
                        *peer,
                        PeerMessage::SyncMessage(message),
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
            ServiceMessage::ReceivedMessage(peer, message) => match message {
                PeerMessage::SyncMessage(message) => {
                    self.on_replication_message(peer, message).await;
                }
                PeerMessage::Announce(message) => {
                    self.on_announcement_message(peer, message).await;
                }
            },
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
        // Generate our own first announcement
        self.update_announcement().await;

        // Subscribe to updates when our target set got changed
        let mut schema_provider_rx = self.schema_provider.on_schema_added();

        loop {
            tokio::select! {
                // Service message arrived
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

                // Target set got updated
                Ok(_) = schema_provider_rx.recv() => {
                    self.update_announcement().await;
                },

                // Announcement & replication schedule is due
                Some(_) = self.scheduler.next() => {
                    self.on_update().await;
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
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::schema::{SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::bus::ServiceMessage;
    use crate::network::{Peer, PeerMessage};
    use crate::replication::service::PeerStatus;
    use crate::replication::{
        Announcement, AnnouncementMessage, Message, Mode, SyncMessage, TargetSet,
    };
    use crate::schema::SchemaProvider;
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
            manager.update_announcement().await;

            // Inform connection manager about new peer
            let remote_peer = Peer::new(remote_peer_id, ConnectionId::new_unchecked(1));

            manager
                .handle_service_message(ServiceMessage::PeerConnected(remote_peer))
                .await;

            let status = manager
                .peers
                .get(&remote_peer)
                .expect("Peer to be registered in connection manager")
                .clone();
            assert_eq!(manager.peers.len(), 1);
            assert_eq!(status.peer, remote_peer);
            assert!(status.sent_our_announcement_timestamp > 0);

            // Manager announces target set with peer
            assert_eq!(rx.len(), 1);
            assert_eq!(
                rx.recv().await,
                Ok(ServiceMessage::SentMessage(
                    remote_peer,
                    PeerMessage::Announce(AnnouncementMessage::new(Announcement::new(
                        target_set.clone()
                    )))
                ))
            );

            // Peer informs us about its target set
            assert_eq!(status.announcement, None);
            let announcement = Announcement::new(target_set.clone());
            manager
                .handle_service_message(ServiceMessage::ReceivedMessage(
                    remote_peer.clone(),
                    PeerMessage::Announce(AnnouncementMessage::new(announcement.clone())),
                ))
                .await;
            let status = manager
                .peers
                .get(&remote_peer)
                .expect("Peer to be registered in connection manager")
                .clone();
            assert_eq!(status.announcement, Some(announcement.clone()));

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

    #[rstest]
    fn unsupported_schema(#[from(random_document_view_id)] document_view_id: DocumentViewId) {
        let local_peer_id =
            PeerId::from_str("12D3KooWD3JAiSNrVGxjC7vJCcjwS8egbtJV9kzrstxLRKiwb9UY").unwrap();
        let remote_peer_id =
            PeerId::from_str("12D3KooWCqtLMJQLY3sm9rpDampJ2nPLswPPZto3mrRY7794QATF").unwrap();

        test_runner(move |node: TestNode| async move {
            let (tx, mut rx) = broadcast::channel::<ServiceMessage>(10);

            let schema_provider = SchemaProvider::new(vec![], Some(vec![]));
            let mut manager =
                ConnectionManager::new(&schema_provider, &node.context.store, &tx, local_peer_id);
            manager.update_announcement().await;

            let remote_peer = Peer::new(remote_peer_id, ConnectionId::new_unchecked(1));

            manager
                .peers
                .insert(remote_peer, PeerStatus::new(remote_peer));

            let unsupported_schema_id = SchemaId::new_application(
                &SchemaName::new("bad_schema").unwrap(),
                &document_view_id,
            );
            let unsupported_target_set = TargetSet::new(&[unsupported_schema_id]);
            manager
                .handle_service_message(ServiceMessage::ReceivedMessage(
                    remote_peer,
                    PeerMessage::SyncMessage(SyncMessage::new(
                        0,
                        Message::SyncRequest(Mode::LogHeight, unsupported_target_set),
                    )),
                ))
                .await;

            assert_eq!(rx.len(), 1);
            assert_eq!(
                rx.recv().await,
                Ok(ServiceMessage::ReplicationFailed(remote_peer))
            );

            assert_eq!(manager.peers.len(), 1);
            assert_eq!(manager.sync_manager.get_sessions(&remote_peer).len(), 0);
        });
    }
}
