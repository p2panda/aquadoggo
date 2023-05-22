// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, StrategyResult, TargetSet};

#[derive(Clone, Debug)]
pub struct SetReconciliationStrategy;

impl SetReconciliationStrategy {
    pub fn new() -> Self {
        Self
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

    async fn initial_messages(&mut self, _store: &SqlStore) -> StrategyResult {
        todo!()
    }

    async fn handle_message(
        &mut self,
        _store: &SqlStore,
        _message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        todo!()
    }
}
