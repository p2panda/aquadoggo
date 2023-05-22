// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, TargetSet};

// @TODO: Better name?!!
#[derive(Clone, Debug)]
pub struct StrategyResult {
    pub messages: Vec<Message>,
    pub is_local_done: bool,
}

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
}

impl NaiveStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
        }
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
        let mut target_set_log_heights = vec![];

        // For every schema id in the target set retrieve log heights for all contributing authors
        for schema_id in self.target_set().0.iter() {
            let log_heights = store
                .get_log_heights(schema_id)
                .await
                .expect("Fatal database error");
            target_set_log_heights.extend(log_heights);
        }
        vec![Message::Have(target_set_log_heights)]
    }

    async fn handle_message(
        &self,
        _store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        // TODO: Verify that the TargetSet contained in the message is a sub-set of the passed
        // local TargetSet.
        let _target_set = self.target_set();
        let messages = Vec::new();
        let mut is_local_done = false;

        match message {
            Message::Have(_log_heights) => {
                // Compose Have message and push to messages
                is_local_done = true;
            }
            Message::Entry(_, _) => {
                // self.handle_entry(..)
            }
            _ => panic!("Naive replication strategy received unsupported message type"),
        }

        Ok(StrategyResult {
            is_local_done,
            messages,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SetReconciliationStrategy();

impl SetReconciliationStrategy {
    pub fn new() -> Self {
        Self()
    }
}

#[async_trait]
impl Strategy for SetReconciliationStrategy {
    fn mode(&self) -> Mode {
        Mode::SetReconciliation
    }

    fn target_set(&self) -> TargetSet {
        todo!()
    }

    async fn initial_messages(&self, _store: &SqlStore) -> Vec<Message> {
        todo!()
    }

    async fn handle_message(
        &self,
        _store: &SqlStore,
        _message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        todo!()
    }
}
