// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use libp2p::PeerId;
use log::{debug, info, warn};
use p2panda_rs::schema::SchemaId;
use tokio::task;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::SqlStore;
use crate::manager::{ServiceReadySender, Shutdown};
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

    let local_peer_id = context
        .config
        .network
        .peer_id
        .expect("Peer id needs to be given");

    // Run a connection manager which deals with the replication logic
    let manager =
        ConnectionManager::new(&context.schema_provider, &context.store, &tx, local_peer_id);
    let handle = task::spawn(manager.run());

    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about replication service being ready");
    };

    tokio::select! {
        _ = handle => (),
        _ = shutdown => {
            // @TODO: Wait until all pending replication processes are completed during graceful
            // shutdown
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerStatus {
    peer_id: PeerId,
    successful_count: usize,
    failed_count: usize,
}

impl PeerStatus {
    pub fn new(peer_id: &PeerId) -> Self {
        Self {
            peer_id: *peer_id,
            successful_count: 0,
            failed_count: 0,
        }
    }
}

struct ConnectionManager {
    /// List of peers the connection mananger knows about and are available for replication.
    peers: HashMap<PeerId, PeerStatus>,

    /// Replication state manager, data ingest and message generator for handling all replication
    /// logic.
    sync_manager: SyncManager<PeerId>,

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
    pub fn new(
        schema_provider: &SchemaProvider,
        store: &SqlStore,
        tx: &ServiceSender,
        local_peer_id: PeerId,
    ) -> Self {
        let ingest = SyncIngest::new(schema_provider.clone(), tx.clone());
        let sync_manager = SyncManager::new(store.clone(), ingest, local_peer_id);
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

    fn remove_connection(&mut self, peer_id: PeerId) {
        match self.peers.remove(&peer_id) {
            Some(_) => debug!("Remove peer: {peer_id}"),
            None => warn!("Tried to remove connection from unknown peer"),
        }
    }

    async fn on_connection_established(&mut self, peer_id: PeerId) {
        info!("Connected to peer: {peer_id}");

        match self.peers.get(&peer_id) {
            Some(_) => {
                warn!("Peer already known: {peer_id}");
            }
            None => {
                self.peers.insert(peer_id, PeerStatus::new(&peer_id));
                self.update_sessions().await;
            }
        }
    }

    async fn on_connection_closed(&mut self, peer_id: PeerId) {
        info!("Disconnected from peer: {peer_id}");

        // Clear running replication sessions from sync manager
        self.sync_manager.remove_sessions(&peer_id);
        self.remove_connection(peer_id)
    }

    async fn on_replication_message(&mut self, peer_id: PeerId, message: SyncMessage) {
        let session_id = message.session_id();

        match self.sync_manager.handle_message(&peer_id, &message).await {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_id, message,
                    ));
                }

                if result.is_done {
                    self.on_replication_finished(peer_id, session_id).await;
                }
            }
            Err(err) => {
                self.on_replication_error(peer_id, session_id, err).await;
            }
        }
    }

    async fn on_replication_finished(&mut self, peer_id: PeerId, _session_id: SessionId) {
        info!("Finished replication with peer {}", peer_id);

        match self.peers.get_mut(&peer_id) {
            Some(status) => {
                status.successful_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }
    }

    async fn on_replication_error(
        &mut self,
        peer_id: PeerId,
        session_id: SessionId,
        error: ReplicationError,
    ) {
        warn!("Replication with peer {} failed: {}", peer_id, error);

        match self.peers.get_mut(&peer_id) {
            Some(status) => {
                status.failed_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }

        match error {
            ReplicationError::StrategyFailed(_) | ReplicationError::Validation(_) => {
                self.sync_manager.remove_session(&peer_id, &session_id);
            }
            _ => (), // Don't try and close the session on other errors as it should not have been initiated
        }
    }

    async fn handle_service_message(&mut self, message: ServiceMessage) {
        match message {
            ServiceMessage::PeerConnected(peer_id) => {
                self.on_connection_established(peer_id).await;
            }
            ServiceMessage::PeerDisconnected(peer_id) => {
                self.on_connection_closed(peer_id).await;
            }
            ServiceMessage::ReceivedReplicationMessage(peer_id, message) => {
                self.on_replication_message(peer_id, message).await;
            }
            _ => (), // Ignore all other messages
        }
    }

    fn send_service_message(&self, message: ServiceMessage) {
        if self.tx.send(message).is_err() {
            // Silently fail here as we don't care if the message was received at this
            // point
        }
    }

    async fn update_sessions(&mut self) {
        // Iterate through all currently connected peers
        let attempt_peers: Vec<PeerId> = self
            .peers
            .clone()
            .into_iter()
            .filter_map(|(peer_id, _)| {
                let sessions = self.sync_manager.get_sessions(&peer_id);
                let active_sessions: Vec<&Session> = sessions
                    .iter()
                    .filter(|session| session.is_done())
                    .collect();

                // Check if we're running too many sessions with that peer on this connection already
                if active_sessions.len() < MAX_SESSIONS_PER_PEER {
                    Some(peer_id)
                } else {
                    debug!("Max sessions reached for peer: {:?}", peer_id);
                    None
                }
            })
            .collect();

        if attempt_peers.is_empty() {
            debug!("No peers available for replication")
        }

        for peer_id in attempt_peers {
            self.initiate_replication(&peer_id).await;
        }
    }

    async fn initiate_replication(&mut self, peer_id: &PeerId) {
        let target_set = self.target_set().await;

        match self
            .sync_manager
            .initiate_session(peer_id, &target_set, &Mode::Naive)
            .await
        {
            Ok(messages) => {
                for message in messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        *peer_id, message,
                    ));
                }
            }
            Err(err) => {
                warn!("Replication error: {}", err)
            }
        }
    }

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
