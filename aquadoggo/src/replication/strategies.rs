// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;

use crate::replication::traits::Strategy;
use crate::replication::{Mode, StrategyMessage, TargetSet};

#[derive(Clone, Debug)]
pub struct StrategyResult {
    is_done: bool,
    messages: Vec<StrategyMessage>,
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

    async fn initial_messages(&self) -> Vec<StrategyMessage> {
        // TODO: Access the store and compose a have message which contains our local log heights over
        // the TargetSet.
        let _target_set = self.target_set();

        vec![StrategyMessage::Have]
    }

    async fn handle_message(&self, message: StrategyMessage) -> Result<StrategyResult> {
        // TODO: Verify that the TargetSet contained in the message is a sub-set of the passed
        // local TargetSet.
        let _target_set = self.target_set();
        let messages = Vec::new();
        let mut is_done = false;

        match message {
            StrategyMessage::Have => {
                // Compose Have message and push to messages
                is_done = true;
            }
            StrategyMessage::Entry => {
                // self.handle_entry(..)
            }
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

    async fn initial_messages(&self) -> Vec<StrategyMessage> {
        todo!()
    }

    async fn handle_message(&self, _message: StrategyMessage) -> Result<StrategyResult> {
        todo!()
    }
}
