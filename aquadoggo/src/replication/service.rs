// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use anyhow::Result;
use libp2p::swarm::ConnectionId;
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

#[derive(PartialEq, Eq, PartialOrd, Clone, Debug, Hash)]

/// Identifier for a connection to another peer. The `ConnectionId` is optional as we need to
/// identify the local peer in the replication manager, but in this case there is no single
/// connection to associate with.
///
/// @TODO: This could be modelled better maybe...
pub struct PeerConnectionId(PeerId, Option<ConnectionId>);

impl Display for PeerConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{:?}",
            self.0,
            // If we don't pass a connection id then we default to `0`
            self.1.unwrap_or(ConnectionId::new_unchecked(0))
        )
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerConnections {
    status: PeerStatus,
    connections: Vec<ConnectionId>,
}

struct ConnectionManager {
    connections: HashMap<PeerId, PeerConnections>,
    sync_manager: SyncManager<PeerConnectionId>,
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
        let sync_manager =
            SyncManager::new(store.clone(), ingest, PeerConnectionId(local_peer_id, None));

        Self {
            connections: HashMap::new(),
            sync_manager,
            tx: tx.clone(),
            rx: tx.subscribe(),
            target_set,
        }
    }

    fn remove_connection(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let peer = self.connections.get_mut(&peer_id);

        match peer {
            Some(peer) => {
                if !peer
                    .connections
                    .iter().any(|id| id == &connection_id)
                {
                    debug!("Tried to remove unknown connection: {peer_id} {connection_id:?}");
                } else {
                    debug!("Remove connection: {peer_id} {connection_id:?}");

                    peer.connections = peer
                        .connections
                        .iter()
                        .filter(|id| *id != &connection_id)
                        .map(ConnectionId::to_owned)
                        .collect();
                };
            }
            None => {
                warn!("Tried to remove connection from unknown peer");
            }
        }
    }

    async fn on_connection_established(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        info!("Connection established with peer: {}", peer_id);
        let peer = self.connections.get_mut(&peer_id);

        match peer {
            Some(peer) => {
                if peer
                    .connections
                    .iter()
                    .any(|id| id == &connection_id)
                {
                    warn!("Duplicate established connection encountered");
                } else {
                    peer.connections.push(connection_id)
                }
            }
            None => {
                let peer_connections = PeerConnections {
                    status: PeerStatus::new(&peer_id),
                    connections: vec![connection_id],
                };
                self.connections.insert(peer_id, peer_connections);
            }
        }

        self.update_sessions().await;
    }

    async fn on_connection_closed(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        // Clear running replication sessions from sync manager
        info!("Connection closed: remove sessions with peer: {}", peer_id);

        let peer_connection_id = PeerConnectionId(peer_id, Some(connection_id));
        self.sync_manager.remove_sessions(&peer_connection_id);
        self.remove_connection(peer_id, connection_id)
    }

    async fn on_replication_message(
        &mut self,
        peer_id: PeerId,
        message: SyncMessage,
        connection_id: ConnectionId,
    ) {
        let session_id = message.session_id();

        match self
            .sync_manager
            .handle_message(&PeerConnectionId(peer_id, Some(connection_id)), &message)
            .await
        {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_id,
                        connection_id,
                        message,
                    ));
                }

                if result.is_done {
                    self.on_replication_finished(peer_id, connection_id, session_id)
                        .await;
                }
            }
            Err(err) => {
                self.on_replication_error(peer_id, connection_id, session_id, err)
                    .await;
            }
        }
    }

    async fn on_replication_finished(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        _session_id: SessionId,
    ) {
        info!("Finished replication with peer {}", peer_id);
        match self.connections.get_mut(&peer_id) {
            Some(peer) => {
                peer.status.successful_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }
    }

    async fn on_replication_error(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        session_id: SessionId,
        error: ReplicationError,
    ) {
        warn!("Replication with peer {} failed: {}", peer_id, error);

        match self.connections.get_mut(&peer_id) {
            Some(peer) => {
                peer.status.failed_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }

        match error {
            ReplicationError::StrategyFailed(_) | ReplicationError::Validation(_) => {
                let peer_connection_id = PeerConnectionId(peer_id, Some(connection_id));
                self.sync_manager
                    .remove_session(&peer_connection_id, &session_id);
            }
            _ => (), // Don't try and close the session on other errors as it should not have been initiated
        }
    }

    async fn handle_service_message(&mut self, message: ServiceMessage) {
        match message {
            ServiceMessage::ConnectionEstablished(peer_id, connection_id) => {
                self.on_connection_established(peer_id, connection_id).await;
            }
            ServiceMessage::ConnectionClosed(peer_id, connection_id) => {
                self.on_connection_closed(peer_id, connection_id).await;
            }
            ServiceMessage::ReceivedReplicationMessage(peer_id, connection_id, message) => {
                self.on_replication_message(peer_id, message, connection_id)
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
        let attempt_peers: Vec<PeerConnectionId> = self
            .connections
            .iter()
            .flat_map(|(peer_id, peer_connections)| {
                // Find out how many sessions we know about for each peer
                let mut connections = vec![];
                for connection_id in &peer_connections.connections {
                    let peer_connection_id = PeerConnectionId(*peer_id, Some(*connection_id));
                    let sessions = self.sync_manager.get_sessions(&peer_connection_id);
                    let active_sessions: Vec<&Session> = sessions
                        .iter()
                        .filter(|session| session.is_done())
                        .collect();

                    // Check if we're running too many sessions with that peer on this connection already
                    if active_sessions.len() < MAX_SESSIONS_PER_CONNECTION {
                        connections.push(peer_connection_id.to_owned());
                    } else {
                        debug!(
                            "Max sessions reached for connection: {:?}",
                            peer_connection_id
                        );
                    }
                }
                connections
            })
            .collect();

        if attempt_peers.is_empty() {
            info!("No peers available for replication")
        }

        for peer_connection_id in attempt_peers {
            self.initiate_replication(&peer_connection_id).await;
        }
    }

    async fn initiate_replication(&mut self, peer_connection_id: &PeerConnectionId) {
        match self
            .sync_manager
            .initiate_session(peer_connection_id, &self.target_set, &Mode::Naive)
            .await
        {
            Ok(messages) => {
                for message in messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_connection_id.0,
                        peer_connection_id
                            .1
                            .expect("Remote peer found without a connection id"),
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
