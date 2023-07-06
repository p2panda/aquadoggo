// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::EncodedOperation;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{
    LogHeightStrategy, Message, Mode, SetReconciliationStrategy, StrategyResult, TargetSet,
};

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
            Mode::LogHeight => Box::new(LogHeightStrategy::new(target_set)),
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

    #[allow(dead_code)]
    pub fn is_live_mode(&self) -> bool {
        self.is_local_live_mode && self.is_remote_live_mode
    }

    #[allow(dead_code)]
    pub fn is_pending(&self) -> bool {
        self.state == SessionState::Pending
    }

    #[allow(dead_code)]
    pub fn is_established(&self) -> bool {
        self.state == SessionState::Established
    }

    pub fn is_done(&self) -> bool {
        self.state == SessionState::Done
    }

    pub fn mode(&self) -> Mode {
        self.strategy.mode()
    }

    pub fn target_set(&self) -> TargetSet {
        self.strategy.target_set()
    }

    /// Send `SyncDone` message last as soon as the done flag flipped.
    fn flippy_flaggy(&mut self, result: &mut StrategyResult) {
        if result.is_local_done && !self.is_local_done {
            result
                .messages
                .push(Message::SyncDone(self.is_local_live_mode));

            self.is_local_done = result.is_local_done;
        }
    }

    pub async fn initial_messages(&mut self, store: &SqlStore) -> Vec<Message> {
        let mut result = self.strategy.initial_messages(store).await;
        self.flippy_flaggy(&mut result);
        result.messages
    }

    /// Validate entry and operation.
    ///
    /// This checks if the received data is actually what we've asked for.
    // @TODO: Make error type smaller in size
    #[allow(clippy::result_large_err)]
    pub fn validate_entry(
        &self,
        _entry_bytes: &EncodedEntry,
        operation_bytes: Option<&EncodedOperation>,
    ) -> Result<(), ReplicationError> {
        if let Some(operation_bytes) = operation_bytes {
            let operation = decode_operation(operation_bytes).map_err(|_| {
                ReplicationError::StrategyFailed("Could not decode operation".into())
            })?;

            if !self.target_set().contains(operation.schema_id()) {
                return Err(ReplicationError::UnmatchedTargetSet);
            }
        }

        Ok(())
    }

    pub async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<Vec<Message>, ReplicationError> {
        let result = match message {
            Message::SyncDone(live_mode) => {
                self.is_remote_done = true;
                self.is_remote_live_mode = *live_mode;
                vec![]
            }
            message => {
                let mut result = self.strategy.handle_message(store, message).await?;
                self.flippy_flaggy(&mut result);
                result.messages
            }
        };

        // As soon as we've received any message from the remote peer we can consider the session
        // to be "established"
        if self.state == SessionState::Pending {
            self.state = SessionState::Established;
        }

        // If local and remote peer decided they're done, we can consider this whole session to be
        // "done"
        if self.is_local_done && self.is_remote_done {
            self.state = SessionState::Done;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::test_utils::memory_store::helpers::{populate_store, PopulateStoreConfig};
    use rstest::rstest;

    use crate::replication::manager::INITIAL_SESSION_ID;
    use crate::replication::{Message, Mode, SessionState, TargetSet};
    use crate::test_utils::helpers::random_target_set;
    use crate::test_utils::{populate_store_config, test_runner, TestNode, populate_and_materialize, test_runner_with_manager, TestNodeManager};

    use super::Session;

    #[rstest]
    fn state_machine(#[from(random_target_set)] target_set: TargetSet) {
        test_runner(move |node: TestNode| async move {
            let mut session = Session::new(
                &INITIAL_SESSION_ID,
                &target_set,
                &Mode::LogHeight,
                true,
                false,
            );
            assert!(!session.is_local_done);
            assert!(!session.is_local_live_mode);
            assert!(!session.is_remote_live_mode);
            assert!(!session.is_remote_done);
            assert!(session.state == SessionState::Pending);

            session
                .handle_message(&node.context.store, &Message::SyncDone(true))
                .await
                .expect("No errors expected");
            assert!(!session.is_local_done);
            assert!(!session.is_local_live_mode);
            assert!(session.is_remote_live_mode);
            assert!(session.is_remote_done);
            assert!(session.state == SessionState::Established);
        })
    }

    #[rstest]
    fn correct_strategy_messages(
        #[from(populate_store_config)]
        #[with(5, 2, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager | async move {
            let target_set = TargetSet::new(&vec![config.schema.id().to_owned()]);
            let mut session = Session::new(
                &INITIAL_SESSION_ID,
                &target_set,
                &Mode::LogHeight,
                true,
                false,
            );

            let mut node_a = manager.create().await;
            populate_and_materialize(&mut node_a, &config).await;

            let response_messages = session
                .handle_message(&node_a.context.store, &Message::Have(vec![]))
                .await
                .unwrap();

            // This node has materialized their documents already so we expect the following
            // messages.
            //
            // 1x Have + 10x Entry + 1x SyncDone = 12 messages
            assert_eq!(response_messages.len(), 12);

            let target_set = TargetSet::new(&vec![config.schema.id().to_owned()]);
            let mut session = Session::new(
                &INITIAL_SESSION_ID,
                &target_set,
                &Mode::LogHeight,
                true,
                false,
            );

            let node_b: TestNode = manager.create().await;
            populate_store(&node_b.context.store, &config).await;

            let response_messages = session
                .handle_message(&node_b.context.store, &Message::Have(vec![]))
                .await
                .unwrap();

            // This node has not materialized any documents so they can't compose their return
            // entries yet.
            //
            // 1x Have + 1x SyncDone = 2 messages
            assert_eq!(response_messages.len(), 2);
        });
    }
}
