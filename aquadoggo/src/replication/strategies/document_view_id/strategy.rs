// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use log::trace;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::traits::AsEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::Human;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{LogHeights, Message, Mode, StrategyResult, TargetSet};

#[derive(Clone, Debug)]
pub struct DocumentViewIdStrategy {
    target_set: TargetSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl DocumentViewIdStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }

    async fn local_view_ids(&self, store: &SqlStore) -> Vec<DocumentViewId> {
        // Collect current view ids for all documents we have materialized for the specified
        // target set.
        let mut document_view_ids = Vec::new();
        for schema_id in self.target_set().iter() {
            let documents = store
                .get_documents_by_schema(schema_id)
                .await
                .expect("Fatal database error");
            document_view_ids.extend(
                documents
                    .iter()
                    .map(|document| document.view_id().to_owned()),
            );
        }
        document_view_ids
    }
}

#[async_trait]
impl Strategy for DocumentViewIdStrategy {
    fn mode(&self) -> Mode {
        Mode::LogHeight
    }

    fn target_set(&self) -> TargetSet {
        self.target_set.clone()
    }

    async fn initial_messages(&mut self, store: &SqlStore) -> StrategyResult {
        let document_view_ids = self.local_view_ids(store).await;
        self.sent_have = true;

        StrategyResult {
            is_local_done: document_view_ids.is_empty(),
            messages: vec![Message::HaveDocuments(document_view_ids)],
        }
    }

    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        let mut result = StrategyResult {
            is_local_done: false,
            messages: vec![],
        };

        // Send our Have message to remote if we haven't done it yet
        if !self.sent_have {
            result.merge(self.initial_messages(store).await);
        }

        match message {
            Message::HaveDocuments(remote_document_view_ids) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                let local_document_view_ids = self.local_view_ids(store).await;

                // @TODO: Calculate entries we should respond with.

                // result.messages.extend(response);
                result.is_local_done = true;

                self.received_remote_have = true;
            }
            _ => {
                return Err(ReplicationError::StrategyFailed(
                    "Received unknown message type".into(),
                ));
            }
        }

        Ok(result)
    }
}
