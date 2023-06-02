// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use libp2p::PeerId;
use log::{debug, info, warn};
use p2panda_rs::schema::SchemaId;
use tokio::sync::broadcast::Receiver;
use tokio::task;
use tokio::time::sleep;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::SqlStore;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::replication::errors::ReplicationError;
use crate::replication::{Mode, Session, SyncIngest, SyncManager, SyncMessage, TargetSet};
use crate::schema::SchemaProvider;

use super::SessionId;

const MAX_SESSIONS_PER_CONNECTION: usize = 3;

pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    // Subscribe to communication bus
    let _rx = tx.subscribe();

    let local_peer_id = context
        .config
        .network
        .peer_id
        .expect("Peer id needs to be given");

    // Define set of schema ids we are interested in
    let supported_schema_ids: Vec<SchemaId> = context
        .schema_provider
        .all()
        .await
        .iter()
        .map(|schema| schema.id().to_owned())
        .collect();
    let target_set = TargetSet::new(&supported_schema_ids);

    // Run a connection manager which deals with the replication logic
    let manager = ConnectionManager::new(
        &context.schema_provider,
        &context.store,
        &tx,
        local_peer_id,
        target_set,
    );

    let scheduler_handle = task::spawn(ConnectionManager::start_scheduler(tx.clone()));
    let handle = task::spawn(manager.run());

    if tx_ready.send(()).is_err() {
        warn!("No subscriber informed about replication service being ready");
    };

    tokio::select! {
        _ = handle => (),
        _ = scheduler_handle => (),
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
    peers: HashMap<PeerId, PeerStatus>,
    sync_manager: SyncManager<PeerId>,
    tx: ServiceSender,
    rx: Receiver<ServiceMessage>,
    target_set: TargetSet,
}

impl ConnectionManager {
    pub fn new(
        schema_provider: &SchemaProvider,
        store: &SqlStore,
        tx: &ServiceSender,
        local_peer_id: PeerId,
        target_set: TargetSet,
    ) -> Self {
        let ingest = SyncIngest::new(schema_provider.clone(), tx.clone());
        let sync_manager = SyncManager::new(store.clone(), ingest, local_peer_id);

        Self {
            peers: HashMap::new(),
            sync_manager,
            tx: tx.clone(),
            rx: tx.subscribe(),
            target_set,
        }
    }

    fn remove_connection(&mut self, peer_id: PeerId) {
        match self.peers.remove(&peer_id) {
            Some(_) => debug!("Remove peer: {peer_id}"),
            None => warn!("Tried to remove connection from unknown peer"),
        }
    }

    async fn on_connection_established(&mut self, peer_id: PeerId) {
        info!("Connection established with peer: {}", peer_id);
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
        // Clear running replication sessions from sync manager
        info!("Connection closed: remove sessions with peer: {}", peer_id);

        self.sync_manager.remove_sessions(&peer_id);
        self.remove_connection(peer_id)
    }

    async fn on_replication_message(
        &mut self,
        peer_id: PeerId,
        message: SyncMessage,
    ) {
        let session_id = message.session_id();

        match self.sync_manager.handle_message(&peer_id, &message).await {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_id,
                        message,
                    ));
                }

                if result.is_done {
                    self.on_replication_finished(peer_id, session_id)
                        .await;
                }
            }
            Err(err) => {
                self.on_replication_error(peer_id, session_id, err)
                    .await;
            }
        }
    }

    async fn on_replication_finished(
        &mut self,
        peer_id: PeerId,
        _session_id: SessionId,
    ) {
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
            ServiceMessage::ConnectionEstablished(peer_id) => {
                self.on_connection_established(peer_id).await;
            }
            ServiceMessage::ConnectionClosed(peer_id) => {
                self.on_connection_closed(peer_id).await;
            }
            ServiceMessage::ReceivedReplicationMessage(peer_id, message) => {
                self.on_replication_message(peer_id, message)
                    .await;
            }
            ServiceMessage::InitiateReplication => {
                self.update_sessions().await;
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
                if active_sessions.len() < MAX_SESSIONS_PER_CONNECTION {
                    Some(peer_id)
                } else {
                    debug!("Max sessions reached for peer: {:?}", peer_id);
                    None
                }
            })
            .collect();

        if attempt_peers.is_empty() {
            info!("No peers available for replication")
        }

        for peer_id in attempt_peers {
            self.initiate_replication(&peer_id).await;
        }
    }

    async fn initiate_replication(&mut self, peer_id: &PeerId) {
        match self
            .sync_manager
            .initiate_session(peer_id, &self.target_set, &Mode::Naive)
            .await
        {
            Ok(messages) => {
                for message in messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_id.clone(),
                        message,
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
            match self.rx.recv().await {
                Ok(message) => self.handle_service_message(message).await,
                Err(err) => {
                    panic!("Service bus subscriber failed: {}", err);
                }
            }
        }
    }

    pub async fn start_scheduler(tx: ServiceSender) {
        loop {
            sleep(Duration::from_secs(5)).await;
            if tx.send(ServiceMessage::InitiateReplication).is_err() {
                // Silently fail here as we don't care if the message was received at this
                // point
            }
        }
    }
}
