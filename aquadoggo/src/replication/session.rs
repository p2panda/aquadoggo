// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::schema::SchemaId;

use crate::replication::traits::Strategy;
use crate::replication::{
    Mode, NaiveStrategy, SetReconciliationStrategy, StrategyMessage, TargetSet,
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
    // @TODO: Access to the store
    // store: Store
    pub id: SessionId,
    pub state: SessionState,
    pub strategy: Box<dyn Strategy>,
}

impl Session {
    pub fn new(id: &SessionId, target_set: &TargetSet, mode: &Mode) -> Self {
        match mode {
            Mode::Naive => {
                let strategy = Box::new(NaiveStrategy::new(target_set, mode));
                return Self {
                    id: id.clone(),
                    state: SessionState::Pending,
                    strategy,
                };
            }
            Mode::SetReconciliation => {
                let strategy = Box::new(SetReconciliationStrategy::new());
                return Self {
                    id: id.clone(),
                    state: SessionState::Pending,
                    strategy,
                };
            }
            Mode::Unknown => panic!("Unknown replication mode found"),
        }
    }

    pub fn mode(&self) -> Mode {
        self.strategy.mode()
    }

    pub fn target_set(&self) -> TargetSet {
        self.strategy.target_set()
    }
}
