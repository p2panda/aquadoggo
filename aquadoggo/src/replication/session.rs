// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::schema::SchemaId;

use crate::replication::{
    NaiveStrategy, ReplicationMode, SetReconciliationStrategy, Strategy, StrategyMessage, TargetSet,
};

pub type SessionId = u64;

#[derive(Clone, Debug)]
pub enum SessionState {
    Pending,
    Established,
    Done,
}

#[derive(Debug)]
pub struct Session {
    // @TODO: Access to the store
    // store: Store
    pub id: SessionId,
    pub state: SessionState,
    pub strategy: Box<dyn Strategy>,
}

impl Session {
    pub fn new(id: &SessionId, target_set: &TargetSet, mode: ReplicationMode) -> Self {
        match mode {
            ReplicationMode::Naive => {
                let strategy = Box::new(NaiveStrategy {
                    mode,
                    target_set: target_set.clone(),
                });
                return Session {
                    id: id.clone(),
                    state: SessionState::Pending,
                    strategy,
                };
            }
            ReplicationMode::SetReconciliation => {
                let strategy = Box::new(SetReconciliationStrategy());
                return Session {
                    id: id.clone(),
                    state: SessionState::Pending,
                    strategy,
                };
            }
        }
    }

    pub fn mode(&self) -> &ReplicationMode {
        &self.strategy.mode()
    }

    pub fn target_set(&self) -> &TargetSet {
        &self.strategy.target_set()
    }
}
