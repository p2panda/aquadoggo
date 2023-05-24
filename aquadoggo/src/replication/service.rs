// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use libp2p::PeerId;
use log::warn;
use p2panda_rs::schema::SchemaId;
use tokio::sync::broadcast::Receiver;
use tokio::task;

use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::SqlStore;
use crate::manager::{ServiceReadySender, Shutdown};
use crate::replication::errors::ReplicationError;
use crate::replication::{Mode, Session, SyncIngest, SyncManager, SyncMessage, TargetSet};
use crate::schema::SchemaProvider;

const MAX_SESSIONS_PER_PEER: usize = 3;

pub async fn replication_service(
    context: Context,
    shutdown: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
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

struct PeerStatus {
    peer_id: PeerId,
    successful_count: usize,
    failed_count: usize,
}

impl PeerStatus {
    pub fn new(peer_id: &PeerId) -> Self {
        Self {
            peer_id: peer_id.clone(),
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

    async fn on_connection_established(&mut self, peer_id: PeerId) {
        if self
            .peers
            .insert(peer_id, PeerStatus::new(&peer_id))
            .is_some()
        {
            warn!("Duplicate established connection encountered");
        }

        self.update_sessions().await;
    }

    async fn on_connection_closed(&mut self, peer_id: PeerId) {
        // Clear running replication sessions from sync manager
        self.sync_manager.remove_sessions(&peer_id);

        // Remove peer from our connections table
        if self.peers.remove(&peer_id).is_none() {
            warn!("Tried to remove unknown connection");
        }

        self.update_sessions().await;
    }

    async fn on_replication_message(&mut self, peer_id: PeerId, message: SyncMessage) {
        match self.sync_manager.handle_message(&peer_id, &message).await {
            Ok(result) => {
                for message in result.messages {
                    self.send_service_message(ServiceMessage::SentReplicationMessage(
                        peer_id, message,
                    ));
                }

                if result.is_done {
                    self.on_replication_finished(peer_id).await;
                }
            }
            Err(err) => {
                self.on_replication_error(peer_id, err).await;
            }
        }
    }

    async fn on_replication_finished(&mut self, peer_id: PeerId) {
        match self.peers.get_mut(&peer_id) {
            Some(status) => {
                status.successful_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }

        self.update_sessions().await;
    }

    async fn on_replication_error(&mut self, peer_id: PeerId, _error: ReplicationError) {
        match self.peers.get_mut(&peer_id) {
            Some(status) => {
                status.failed_count += 1;
            }
            None => {
                panic!("Tried to access unknown peer");
            }
        }

        // @TODO: SyncManager should remove session internally on critical errors

        self.update_sessions().await;
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
                self.on_replication_message(peer_id, message).await;
            }
            _ => (), // Ignore all other messages
        }
    }

    fn send_service_message(&mut self, message: ServiceMessage) {
        if self.tx.send(message).is_err() {
            // Silently fail here as we don't care if the message was received at this
            // point
        }
    }

    async fn update_sessions(&mut self) {
        // Iterate through all currently connected peers
        let attempt_peers: Vec<PeerId> = self
            .peers
            .iter()
            .filter_map(|(peer_id, _peer_status)| {
                // Find out how many sessions we know about for each peer
                let sessions = self.sync_manager.get_sessions(&peer_id);
                let active_sessions: Vec<&Session> = sessions
                    .iter()
                    .filter(|session| session.is_done())
                    .collect();

                // Check if we're running too many sessions with that peer already
                if active_sessions.len() < MAX_SESSIONS_PER_PEER {
                    return Some(peer_id.to_owned());
                }

                return None;
            })
            .collect();

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
            Err(_err) => {
                // @TODO
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
}
