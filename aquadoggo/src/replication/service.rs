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
use crate::replication::{SyncIngest, SyncManager, TargetSet};
use crate::schema::SchemaProvider;

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

    fn send_service_message(&mut self, message: ServiceMessage) {
        if self.tx.send(message).is_err() {
            // Silently fail here as we don't care if the message was received at this
            // point
        }
    }

    async fn handle_service_message(&mut self, message: ServiceMessage) {
        match message {
            ServiceMessage::ConnectionEstablished(peer_id) => {
                // @TODO
            }
            ServiceMessage::ConnectionClosed(peer_id) => {
                // @TODO
            }
            ServiceMessage::ReceivedReplicationMessage(peer_id, message) => {
                match self.sync_manager.handle_message(&peer_id, &message).await {
                    Ok(result) => {
                        for message in result.messages {
                            self.send_service_message(ServiceMessage::SentReplicationMessage(
                                peer_id, message,
                            ));
                        }

                        if result.is_done {
                            // @TODO
                        }
                    }
                    Err(err) => {
                        // @TODO
                    }
                }
            }
            _ => (), // Ignore all other messages
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
