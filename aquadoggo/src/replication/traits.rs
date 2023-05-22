// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::EncodedOperation;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::{Message, Mode, StrategyResult, TargetSet};

#[async_trait]
pub trait Strategy: std::fmt::Debug + StrategyClone + Sync + Send {
    /// Replication mode of this strategy.
    fn mode(&self) -> Mode;

    /// Target set replication is occurring over.
    fn target_set(&self) -> TargetSet;

    /// Generate initial messages.
    async fn initial_messages(&self, store: &SqlStore) -> Vec<Message>;

    /// Handle incoming message and return response.
    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError>;

    /// Validate and store entry and operation.
    ///
    /// This checks if the received data is actually what we've asked for.
    async fn validate_entry(
        &mut self,
        _entry_bytes: &EncodedEntry,
        operation_bytes: &EncodedOperation,
    ) -> Result<(), ReplicationError> {
        let operation = decode_operation(operation_bytes)
            .map_err(|_| ReplicationError::StrategyFailed("Could not decode operation".into()))?;

        if !self.target_set().contains(operation.schema_id()) {
            return Err(ReplicationError::UnmatchedTargetSet);
        }

        Ok(())
    }
}

// This is a little trick so we can clone trait objects.
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
