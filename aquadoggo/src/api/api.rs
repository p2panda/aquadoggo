// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{bail, Result};
use tokio::sync::mpsc::Receiver;

use crate::api::{migrate, LockFile};
use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;

/// Node events which can be interesting for clients, for example when peers connect or disconnect.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// A peer connected to our node. This can be a direct or relayed connection.
    PeerConnected,

    /// A peer disconnected from our node.
    PeerDisconnected,
}

/// Interface to interact with the node in a programmatic, "low-level" way.
#[derive(Debug)]
pub struct NodeInterface {
    context: Context,
    tx: ServiceSender,
}

impl NodeInterface {
    pub fn new(context: Context, tx: ServiceSender) -> Self {
        Self { context, tx }
    }

    pub async fn migrate(&self, lock_file: LockFile) -> Result<bool> {
        let committed_operations = migrate(
            &self.context.store,
            &self.context.schema_provider,
            lock_file,
        )
        .await?;

        let did_migration_happen = !committed_operations.is_empty();

        // Send new operations from migration on service communication bus, this will arrive
        // eventually at the materializer service
        for operation_id in committed_operations {
            if self
                .tx
                .send(ServiceMessage::NewOperation(operation_id))
                .is_err()
            {
                bail!("Failed to inform materialization service about migration");
            }
        }

        Ok(did_migration_happen)
    }

    pub async fn subscribe(&self) -> Receiver<NodeEvent> {
        let mut rx = self.tx.subscribe();
        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<NodeEvent>(256);

        tokio::task::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ServiceMessage::PeerConnected(_)) => {
                        let _ = events_tx.send(NodeEvent::PeerConnected).await;
                    }
                    Ok(ServiceMessage::PeerDisconnected(_)) => {
                        let _ = events_tx.send(NodeEvent::PeerDisconnected).await;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        events_rx
    }
}
