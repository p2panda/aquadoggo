// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use log::trace;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::traits::AsEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::Human;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::document::diff_documents;
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

    async fn local_documents(&self, store: &SqlStore) -> Vec<impl AsDocument> {
        // Collect current view ids for all documents we have materialized for the specified
        // target set.
        let mut documents = Vec::new();
        for schema_id in self.target_set().iter() {
            let schema_documents = store
                .get_documents_by_schema(schema_id)
                .await
                .expect("Fatal database error");
            documents.extend(schema_documents.into_iter());
        }
        documents
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
        let documents: Vec<(DocumentId, DocumentViewId)> = self
            .local_documents(store)
            .await
            .iter()
            .map(|document| (document.id().clone(), document.view_id().to_owned()))
            .collect();
        self.sent_have = true;

        StrategyResult {
            is_local_done: documents.is_empty(),
            messages: vec![Message::HaveDocuments(documents)],
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
            Message::HaveDocuments(remote_documents) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                // Get all documents we have locally for the sessions target set.
                let local_documents = self.local_documents(store).await;
                
                // Diff the received documents against what we have locally and calculate what the
                // remote node is missing. This gives us the document height from which the remote
                // node needs updating.
                let remote_requires =
                    diff_documents(store, local_documents, remote_documents.to_owned()).await;

                // Get the actual entries we should sent to the remote.
                let mut entries = Vec::new();
                for (document_id, index) in remote_requires {
                    let document_entries = store
                        .get_entries_from_operation_index(&document_id, index)
                        .await
                        .expect("Fatal database error");
                    entries.extend(document_entries);
                }

                // Parse the entries into the correct message response type.
                let response: Vec<Message> = entries
                    .iter()
                    .map(|entry| {
                        Message::Entry(entry.clone().encoded_entry, entry.payload().cloned())
                    })
                    .collect();

                result.messages.extend(response);
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
