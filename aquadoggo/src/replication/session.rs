// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, NaiveStrategy, SetReconciliationStrategy, TargetSet};

pub type SessionId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
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

    /// True if we're done locally with this replication session.
    pub is_local_done: bool,

    /// True if we received a `SyncDone` message from the remote peer.
    pub is_remote_done: bool,

    /// True if the we support live-mode with remote peer.
    pub is_local_live_mode: bool,

    /// True if the remote peer suggested entering live-mode.
    pub is_remote_live_mode: bool,
}

impl Session {
    pub fn new(
        id: &SessionId,
        target_set: &TargetSet,
        mode: &Mode,
        local: bool,
        live_mode: bool,
    ) -> Self {
        let strategy: Box<dyn Strategy> = match mode {
            Mode::Naive => Box::new(NaiveStrategy::new(target_set)),
            Mode::SetReconciliation => Box::new(SetReconciliationStrategy::new()),
            Mode::Unknown => panic!("Unknown replication mode"),
        };

        Self {
            id: *id,
            state: SessionState::Pending,
            local,
            strategy,
            is_local_done: false,
            is_remote_done: false,
            is_remote_live_mode: false,
            is_local_live_mode: live_mode,
        }
    }

    pub fn live_mode(&self) -> bool {
        self.is_local_live_mode && self.is_remote_live_mode
    }

    pub fn mode(&self) -> Mode {
        self.strategy.mode()
    }

    pub fn target_set(&self) -> TargetSet {
        self.strategy.target_set()
    }

    pub async fn initial_messages(&mut self, store: &SqlStore) -> Vec<Message> {
        self.strategy.initial_messages(store).await
    }

    pub async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<Vec<Message>, ReplicationError> {
        match message {
            Message::SyncDone(live_mode) => {
                self.is_remote_done = true;
                self.is_remote_live_mode = *live_mode;

                if self.is_local_done && self.is_remote_done {
                    self.state = SessionState::Done;
                }

                Ok(vec![])
            }
            message => {
                let result = self.strategy.handle_message(store, message).await?;
                self.is_local_done = result.is_local_done;

                if self.state == SessionState::Pending {
                    self.state = SessionState::Established;
                }

                if self.is_local_done && self.is_remote_done {
                    self.state = SessionState::Done;
                }

                Ok(result.messages)
            }
        }
    }
}
