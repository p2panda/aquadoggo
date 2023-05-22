// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::{Message, Mode, Session, SessionId, SessionState, SyncMessage, TargetSet};

pub const INITIAL_SESSION_ID: SessionId = 0;

pub const SUPPORTED_MODES: [Mode; 1] = [Mode::Naive];

pub const SUPPORT_LIVE_MODE: bool = false;

#[derive(Clone, Debug)]
pub struct SyncResult {
    pub messages: Vec<Message>,
    pub is_done: bool,
}

#[derive(Debug)]
pub struct SyncManager<P> {
    store: SqlStore,
    local_peer: P,
    sessions: HashMap<P, Vec<Session>>,
}

impl<P> SyncManager<P>
where
    P: Clone + std::hash::Hash + Eq + PartialOrd,
{
    pub fn new(store: SqlStore, local_peer: P) -> Self {
        Self {
            store,
            local_peer,
            sessions: HashMap::new(),
        }
    }

    /// Get all sessions related to a remote peer.
    fn get_sessions(&self, remote_peer: &P) -> Vec<Session> {
        self.sessions
            .get(remote_peer)
            // Always return an array, even when it is empty
            .unwrap_or(&vec![])
            .to_owned()
    }

    /// Register a new session in manager.
    async fn insert_session(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        target_set: &TargetSet,
        mode: &Mode,
        local: bool,
    ) -> Vec<Message> {
        let mut session = Session::new(session_id, target_set, mode, local, SUPPORT_LIVE_MODE);
        let initial_messages = session.initial_messages(&self.store).await;

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            self.sessions.insert(remote_peer.clone(), vec![session]);
        }

        initial_messages
    }

    pub async fn initiate_session(
        &mut self,
        remote_peer: &P,
        target_set: &TargetSet,
        mode: &Mode,
    ) -> Result<Vec<Message>, ReplicationError> {
        SyncManager::<P>::is_mode_supported(mode)?;

        let sessions = self.get_sessions(remote_peer);

        // Make sure to not have duplicate sessions over the same schema ids
        let session = sessions
            .iter()
            .find(|session| session.target_set() == *target_set);

        if let Some(session) = session {
            return Err(ReplicationError::DuplicateOutboundRequest(session.id));
        }

        // Determine next id for upcoming session
        let session_id = {
            if let Some(session) = sessions.last() {
                session.id + 1
            } else {
                INITIAL_SESSION_ID
            }
        };

        let initial_messages = self
            .insert_session(remote_peer, &session_id, target_set, mode, true)
            .await;

        let mut all_messages = vec![Message::SyncRequest(mode.clone(), target_set.clone())];
        all_messages.extend(initial_messages);

        Ok(all_messages)
    }

    fn is_mode_supported(mode: &Mode) -> Result<(), ReplicationError> {
        if !SUPPORTED_MODES.contains(mode) {
            return Err(ReplicationError::UnsupportedMode);
        }

        Ok(())
    }

    fn remove_session(&mut self, remote_peer: &P, index: usize) {
        let sessions = self
            .sessions
            .get_mut(remote_peer)
            .expect("Expected at least one pending session");
        sessions.remove(index);
    }

    async fn handle_duplicate_session(
        &mut self,
        remote_peer: &P,
        target_set: &TargetSet,
        index: usize,
        session: &Session,
    ) -> Result<SyncResult, ReplicationError> {
        let accept_inbound_request = match session.state {
            // Handle only duplicate sessions when they haven't started yet
            SessionState::Pending => {
                if &self.local_peer < remote_peer {
                    // Drop our pending session
                    self.remove_session(remote_peer, index);

                    // Accept the inbound request
                    true
                } else {
                    // Keep our pending session, ignore inbound request
                    false
                }
            }
            _ => return Err(ReplicationError::DuplicateInboundRequest(session.id)),
        };

        let mut all_messages = vec![];

        if accept_inbound_request {
            let messages = self
                .insert_session(remote_peer, &session.id, target_set, &session.mode(), false)
                .await;
            all_messages.extend(messages);

            // If we dropped our own outbound session request regarding a different target set, we
            // need to re-establish it with another session id, otherwise it would get lost
            if session.target_set() != *target_set {
                let messages = self
                    .initiate_session(remote_peer, target_set, &session.mode())
                    .await?;
                all_messages.extend(messages)
            }
        }

        Ok(SyncResult {
            is_done: false,
            messages: all_messages,
        })
    }

    async fn handle_sync_request(
        &mut self,
        remote_peer: &P,
        mode: &Mode,
        session_id: &SessionId,
        target_set: &TargetSet,
    ) -> Result<SyncResult, ReplicationError> {
        SyncManager::<P>::is_mode_supported(mode)?;

        let sessions = self.get_sessions(remote_peer);

        // Check if a session with this id already exists for this peer, this can happen if both
        // peers started to initiate a session at the same time, we can try to resolve this
        if let Some((index, session)) = sessions
            .iter()
            .enumerate()
            .find(|(_, session)| session.id == *session_id && session.local)
        {
            return self
                .handle_duplicate_session(remote_peer, target_set, index, session)
                .await;
        }

        // Check if a session with this target set already exists for this peer, this always gets
        // rejected because it is clearly redundant
        if let Some(session) = sessions
            .iter()
            .find(|session| session.target_set() == *target_set)
        {
            return Err(ReplicationError::DuplicateInboundRequest(session.id));
        }

        let messages = self
            .insert_session(remote_peer, session_id, target_set, mode, false)
            .await;

        Ok(SyncResult {
            is_done: false,
            messages,
        })
    }

    async fn handle_session_message(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        message: &Message,
    ) -> Result<SyncResult, ReplicationError> {
        let mut sessions = self.get_sessions(remote_peer);

        // Check if a session exists with the given id for this peer.
        if let Some((index, session)) = sessions
            .iter_mut()
            .enumerate()
            .find(|(_, session)| session.id == *session_id)
        {
            // Pass the message onto the session when found.
            let messages = session.handle_message(&self.store, message).await?;

            // We're done, clean up after ourselves
            if session.state == SessionState::Done {
                self.remove_session(remote_peer, index);
            }

            Ok(SyncResult {
                messages,
                is_done: session.state == SessionState::Done,
            })
        } else {
            Err(ReplicationError::NoSessionFound(*session_id))
        }
    }

    pub async fn handle_message(
        &mut self,
        remote_peer: &P,
        sync_message: &SyncMessage,
    ) -> Result<SyncResult, ReplicationError> {
        match sync_message.message() {
            Message::SyncRequest(mode, target_set) => {
                self.handle_sync_request(remote_peer, mode, &sync_message.session_id(), target_set)
                    .await
            }
            message => {
                self.handle_session_message(remote_peer, &sync_message.session_id(), message)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::replication::errors::ReplicationError;
    use crate::replication::message::Message;
    use crate::replication::{Mode, SyncMessage, TargetSet};
    use crate::test_utils::helpers::random_target_set;
    use crate::test_utils::{test_runner, TestNode};

    use super::{SyncManager, INITIAL_SESSION_ID};

    const PEER_ID_LOCAL: &'static str = "local";
    const PEER_ID_REMOTE: &'static str = "remote";

    #[rstest]
    fn initiate_outbound_session(
        #[from(random_target_set)] target_set_1: TargetSet,
        #[from(random_target_set)] target_set_2: TargetSet,
    ) {
        test_runner(move |node: TestNode| async move {
            let mode = Mode::Naive;

            let mut manager = SyncManager::new(node.context.store.clone(), PEER_ID_LOCAL);
            let result = manager
                .initiate_session(&PEER_ID_REMOTE, &target_set_1, &mode)
                .await;
            assert!(result.is_ok());

            let result = manager
                .initiate_session(&PEER_ID_REMOTE, &target_set_2, &mode)
                .await;
            assert!(result.is_ok());

            // Expect error when initiating a session for the same target set
            let result = manager
                .initiate_session(&PEER_ID_REMOTE, &target_set_1, &mode)
                .await;
            assert!(matches!(
                result,
                Err(ReplicationError::DuplicateOutboundRequest(0))
            ));
        })
    }

    #[rstest]
    fn initiate_inbound_session(
        #[from(random_target_set)] target_set_1: TargetSet,
        #[from(random_target_set)] target_set_2: TargetSet,
    ) {
        test_runner(move |node: TestNode| async move {
            let mut manager = SyncManager::new(node.context.store.clone(), PEER_ID_LOCAL);

            let message =
                SyncMessage::new(0, Message::SyncRequest(Mode::Naive, target_set_1.clone()));
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_ok());

            let message =
                SyncMessage::new(1, Message::SyncRequest(Mode::Naive, target_set_2.clone()));
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_ok());

            // Reject attempt to create session again
            let message =
                SyncMessage::new(0, Message::SyncRequest(Mode::Naive, target_set_1.clone()));
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(matches!(
                result,
                Err(ReplicationError::DuplicateInboundRequest(0))
            ));

            // Reject different session concerning same target set
            let message =
                SyncMessage::new(2, Message::SyncRequest(Mode::Naive, target_set_2.clone()));
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(matches!(
                result,
                Err(ReplicationError::DuplicateInboundRequest(1))
            ));
        })
    }

    #[rstest]
    fn inbound_checks_supported_mode(#[from(random_target_set)] target_set: TargetSet) {
        test_runner(move |node: TestNode| async move {
            // Should not fail when requesting supported replication mode
            let mut manager = SyncManager::new(node.context.store.clone(), PEER_ID_LOCAL);
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::Naive, target_set.clone()),
            );
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_ok());

            // Should fail when requesting unsupported replication mode
            let mut manager = SyncManager::new(node.context.store.clone(), PEER_ID_LOCAL);
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::SetReconciliation, target_set.clone()),
            );
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_err());
        })
    }
}
