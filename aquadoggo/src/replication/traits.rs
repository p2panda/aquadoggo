// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::schema::SchemaId;

use crate::replication::{Mode, Message, StrategyResult, TargetSet};

#[async_trait]
pub trait Strategy: std::fmt::Debug + StrategyClone {
    /// Replication mode of this strategy.
    fn mode(&self) -> Mode;

    /// Target set replication is occurring over.
    fn target_set(&self) -> TargetSet;

    // Generate initial messages.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn initial_messages(&self) -> Vec<Message>;

    // Handle incoming message and return response.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn handle_message(&self, message: Message) -> Result<StrategyResult>;

    // Validate and store entry and operation.
    //
    // @TODO: we want to pass the store in here too eventually.
    async fn handle_entry(
        &self,
        _schema_id: &SchemaId,
        _entry_bytes: Vec<u8>,
        _operation_bytes: Vec<u8>,
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

/// This is a little trick so we can clone trait objects.
pub trait StrategyClone {
    fn clone_box(&self) -> Box<dyn Strategy>;
}

impl<T> StrategyClone for T
where
    T: 'static + Strategy + Clone,
{
    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<dyn Strategy> {
    fn clone(&self) -> Box<dyn Strategy> {
        self.clone_box()
    }
}
