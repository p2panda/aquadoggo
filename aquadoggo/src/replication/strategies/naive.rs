// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::diff_log_heights;
use crate::replication::traits::Strategy;
use crate::replication::{LogHeight, Message, Mode, StrategyResult, TargetSet};

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl NaiveStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }

    async fn local_log_heights(&self, store: &SqlStore) -> Vec<LogHeight> {
        let mut result = vec![];

        // For every schema id in the target set retrieve log heights for all contributing authors
        for schema_id in self.target_set().0.iter() {
            let log_heights = store
                .get_log_heights(schema_id)
                .await
                .expect("Fatal database error");
            result.extend(log_heights);
        }

        result
    }

    async fn handle_have_message(
        &self,
        store: &SqlStore,
        remote_log_heights: &[LogHeight],
    ) -> Vec<Message> {
        let mut messages = Vec::new();

        let local_log_heights = self.local_log_heights(store).await;
        let remote_needs = diff_log_heights(&local_log_heights, remote_log_heights);

        for (public_key, log_heights) in remote_needs {
            for (log_id, seq_num) in log_heights {
                let entry_messages: Vec<Message> = store
                    .get_entries_from(&public_key, &log_id, &seq_num)
                    .await
                    .expect("Fatal database error")
                    .iter()
                    .map(|entry| {
                        Message::Entry(entry.clone().encoded_entry, entry.payload().cloned())
                    })
                    .collect();
                messages.extend(entry_messages);
            }
        }

        messages
    }
}

#[async_trait]
impl Strategy for NaiveStrategy {
    fn mode(&self) -> Mode {
        Mode::Naive
    }

    fn target_set(&self) -> TargetSet {
        self.target_set.clone()
    }

    async fn initial_messages(&self, store: &SqlStore) -> Vec<Message> {
        vec![Message::Have(self.local_log_heights(store).await)]
    }

    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        let mut messages = Vec::new();
        let is_local_done: bool;

        // Send our Have message to remote if we haven't done it yet
        if !self.sent_have {
            messages.extend(self.initial_messages(store).await);
            self.sent_have = true;
        }

        match message {
            Message::Have(remote_log_heights) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                let response = self.handle_have_message(store, remote_log_heights).await;
                messages.extend(response);

                is_local_done = true;
                self.received_remote_have = true;
            }
            _ => {
                return Err(ReplicationError::StrategyFailed(
                    "Received unknown message type".into(),
                ));
            }
        }

        Ok(StrategyResult {
            is_local_done,
            messages,
        })
    }
}
