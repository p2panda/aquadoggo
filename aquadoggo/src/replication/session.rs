// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::db::SqlStore;
use crate::replication::traits::Strategy;
use crate::replication::{
    Message, Mode, NaiveStrategy, SetReconciliationStrategy, StrategyResult, TargetSet,
};

pub type SessionId = u64;

#[derive(Clone, Debug)]
pub enum SessionState {
    Pending,
    Established,
    Done,
}

#[derive(Clone, Debug)]
pub struct Session {
    /// Unique identifier of this session for that peer.
    pub id: SessionId,

    /// Current state of this session.
    pub state: SessionState,

    /// True if session was established by us.
    pub local: bool,

    /// Replication strategy handler.
    pub strategy: Box<dyn Strategy>,
}

impl Session {
    pub fn new(id: &SessionId, target_set: &TargetSet, mode: &Mode, local: bool) -> Self {
        let strategy: Box<dyn Strategy> = match mode {
            Mode::Naive => Box::new(NaiveStrategy::new(target_set, mode)),
            Mode::SetReconciliation => Box::new(SetReconciliationStrategy::new()),
            Mode::Unknown => panic!("Unknown replication mode found"),
        };

        Self {
            id: *id,
            state: SessionState::Pending,
            strategy,
            local,
        }
    }

    pub fn mode(&self) -> Mode {
        self.strategy.mode()
    }

    pub fn target_set(&self) -> TargetSet {
        self.strategy.target_set()
    }

    pub async fn initial_messages(&self, store: &SqlStore) -> Vec<Message> {
        self.strategy.initial_messages(&store).await
    }

    pub async fn handle_message(
        &self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult> {
        self.strategy.handle_message(&store, message).await
    }
}
