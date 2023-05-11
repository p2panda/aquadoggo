// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::schema::SchemaId;

use crate::replication::{Mode, StrategyMessage, TargetSet};

#[derive(Clone, Debug)]
pub struct StrategyResult {
    is_done: bool,
    messages: Vec<StrategyMessage>,
}

#[async_trait]
pub trait Strategy: std::fmt::Debug {
    /// Replication mode of this strategy.
    fn mode(&self) -> Mode;

    /// Target set replication is occurring over.
    fn target_set(&self) -> TargetSet;

    // Generate initial messages.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn initial_messages(&self) -> Vec<StrategyMessage>;

    // Handle incoming message and return response.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn handle_message(&self, message: StrategyMessage) -> Result<StrategyResult>;

    // Validate and store entry and operation.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn handle_entry(
        &self,
        schema_id: &SchemaId,
        entry_bytes: Vec<u8>,
        operation_bytes: Vec<u8>,
    ) -> Result<()> {
        // Validation:
        // Check against schema_id and target_set if entry is what we've asked for
        let _target_set = self.target_set();

        // Further validation through our publish api stuff (?!)

        // Have an entry waiting lobby service here, to batch stuff?!
        // Nice to check certificate pool in one go.
        // Nice to not put too much pressure on the database.
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
    mode: Mode,
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
        let mut messages = Vec::new();
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

    async fn handle_message(&self, message: StrategyMessage) -> Result<StrategyResult> {
        todo!()
    }
}
