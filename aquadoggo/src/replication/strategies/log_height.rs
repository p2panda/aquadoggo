// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use log::trace;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::Human;

use crate::db::types::StorageEntry;
use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::diff_log_heights;
use crate::replication::traits::Strategy;
use crate::replication::{LogHeights, Message, Mode, StrategyResult, TargetSet};

#[derive(Clone, Debug)]
pub struct LogHeightStrategy {
    target_set: TargetSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl LogHeightStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }

    async fn local_log_heights(
        &self,
        store: &SqlStore,
    ) -> HashMap<PublicKey, Vec<(LogId, SeqNum)>> {
        let mut log_heights: HashMap<PublicKey, Vec<(LogId, SeqNum)>> = HashMap::new();

        for schema_id in self.target_set().iter() {
            // For every schema id in the target set retrieve log heights for all contributing authors
            let schema_logs = store
                .get_log_heights(schema_id)
                .await
                .expect("Fatal database error")
                .into_iter();

            // Then merge them into any existing records for the author
            for (public_key, logs) in schema_logs {
                let mut author_logs = log_heights.get(&public_key).cloned().unwrap_or(vec![]);
                author_logs.extend(logs);
                author_logs.sort();
                log_heights.insert(public_key, author_logs);
            }
        }
        log_heights
    }

    async fn entry_responses(
        &self,
        store: &SqlStore,
        remote_log_heights: &[LogHeights],
    ) -> Vec<Message> {
        let mut entries = Vec::<(StorageEntry, DocumentId, i32)>::new();

        // Get local log heights for the configured target set.
        let local_log_heights = self.local_log_heights(store).await;

        // Compare local and remote log heights to determine what they need from us.
        let remote_needs = diff_log_heights(
            &local_log_heights,
            &remote_log_heights.iter().cloned().collect(),
        );

        for (public_key, log_heights) in remote_needs {
            for (log_id, seq_num) in log_heights {
                // Get the entries the remote needs for each log.
                let log_entries = store
                    .get_entries_from(&public_key, &log_id, &seq_num)
                    .await
                    .expect("Fatal database error");

                for entry in log_entries {
                    // Get the entry as well as we need some additional information in order to
                    // send the entries in the correct order.
                    let operation = store
                        .get_operation(&entry.hash().into())
                        .await
                        .expect("Fatal database error")
                        .expect("Operation should be in store");

                    // We only send entries if their operation has been materialized.
                    if let Some(sorted_index) = operation.sorted_index {
                        entries.push((entry, operation.document_id, sorted_index));
                    }
                }
            }
        }

        // Sort all entries by document_id & sorted_index.
        entries.sort_by(
            |(_, document_id_a, sorted_index_a), (_, document_id_b, sorted_index_b)| {
                (document_id_a, sorted_index_a).cmp(&(document_id_b, sorted_index_b))
            },
        );

        // Compose the actual messages.
        entries
            .iter()
            .map(|(entry, _, _)| {
                trace!(
                    "Prepare message containing entry at {:?} on {:?} for {}",
                    entry.seq_num(),
                    entry.log_id(),
                    entry.public_key().display()
                );

                Message::Entry(entry.clone().encoded_entry, entry.payload().cloned())
            })
            .collect()
    }
}

#[async_trait]
impl Strategy for LogHeightStrategy {
    fn mode(&self) -> Mode {
        Mode::LogHeight
    }

    fn target_set(&self) -> TargetSet {
        self.target_set.clone()
    }

    async fn initial_messages(&mut self, store: &SqlStore) -> StrategyResult {
        let log_heights = self.local_log_heights(store).await;
        self.sent_have = true;

        StrategyResult {
            is_local_done: log_heights.is_empty(),
            messages: vec![Message::Have(log_heights.into_iter().collect())],
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
            Message::Have(remote_log_heights) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                let response = self.entry_responses(store, remote_log_heights).await;
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
