// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, TargetSet};

#[derive(Clone, Debug)]
pub struct StrategyResult {
    pub is_done: bool,
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
    mode: Mode,
}

impl NaiveStrategy {
    pub fn new(target_set: &TargetSet, mode: &Mode) -> Self {
        Self {
            target_set: target_set.clone(),
            mode: mode.clone(),
        }
    }
}

#[async_trait]
impl Strategy for NaiveStrategy {
    fn mode(&self) -> Mode {
        self.mode.clone()
    }

    fn target_set(&self) -> TargetSet {
        self.target_set.clone()
    }

    async fn initial_messages(&self, _store: &SqlStore) -> Vec<Message> {
        // TODO: Access the store and compose a have message which contains our local log heights over
        // the TargetSet.
        let _target_set = self.target_set();

        vec![Message::Have(vec![])]
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
        let mut is_done = false;

        match message {
            Message::Have(_log_heights) => {
                // Compose Have message and push to messages
                is_done = true;
            }
            Message::Entry(_, _) => {
                // self.handle_entry(..)
            }
            _ => panic!("Naive replication strategy received unsupported message type"),
        }

        Ok(StrategyResult { is_done, messages })
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
        todo!()
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
