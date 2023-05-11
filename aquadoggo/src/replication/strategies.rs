// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;

use crate::replication::{ReplicationMode, StrategyMessage};

struct StrategyResult {
    is_done: bool,
    messages: Vec<StrategyMessage>,
}

#[async_trait]
pub trait Strategy {
    /// Replication mode of this strategy.
    fn mode() -> ReplicationMode;

    // Generate initial messages.
    async fn initial_messages(&self) -> Vec<StrategyMessage>;

    // Handle incoming message and return response.
    async fn handle_message(&self, message: StrategyMessage) -> Result<StrategyResult>;
}