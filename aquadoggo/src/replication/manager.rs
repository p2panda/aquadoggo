// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;

use crate::replication::errors::ReplicationError;
use crate::replication::{Message, Mode, Session, SessionId, SessionState, SyncMessage, TargetSet};

pub const INITIAL_SESSION_ID: SessionId = 0;

pub const SUPPORTED_MODES: [Mode; 1] = [Mode::Naive];

#[derive(Debug)]
pub struct SyncManager<P> {
    local_peer: P,
    sessions: HashMap<P, Vec<Session>>,
}

impl<P> SyncManager<P>
where
    P: Clone + std::hash::Hash + Eq + PartialOrd,
{
    pub fn new(local_peer: P) -> Self {
        Self {
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
    fn insert_session(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        target_set: &TargetSet,
        mode: &Mode,
        local: bool,
    ) {
        let session = Session::new(session_id, target_set, mode, local);

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            self.sessions.insert(remote_peer.clone(), vec![session]);
        }
    }

    pub fn initiate_session(
        &mut self,
        remote_peer: &P,
        target_set: &TargetSet,
        mode: &Mode,
    ) -> Result<(), ReplicationError> {
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

        self.insert_session(remote_peer, &session_id, target_set, mode, true);

        Ok(())
    }

    fn is_mode_supported(mode: &Mode) -> Result<(), ReplicationError> {
        if !SUPPORTED_MODES.contains(mode) {
            return Err(ReplicationError::UnsupportedMode);
        }

        Ok(())
    }

    fn handle_duplicate_session(
        &mut self,
        remote_peer: &P,
        target_set: &TargetSet,
        index: usize,
        session: &Session,
    ) -> Result<(), ReplicationError> {
        let accept_inbound_request = match session.state {
            // Handle only duplicate sessions when they haven't started yet
            SessionState::Pending => {
                if &self.local_peer < remote_peer {
                    // Drop our pending session
                    let sessions = self
                        .sessions
                        .get_mut(remote_peer)
                        .expect("Expected at least one pending session");
                    sessions.remove(index);

                    // Accept the inbound request
                    true
                } else {
                    // Keep our pending session, ignore inbound request
                    false
                }
            }
            _ => return Err(ReplicationError::DuplicateInboundRequest(session.id)),
        };

        if accept_inbound_request {
            self.insert_session(remote_peer, &session.id, target_set, &session.mode(), false);

            // @TODO: Session needs to generate some messages on creation and
            // it will pass them back up to us to then forward onto
            // the swarm

            // If we dropped our own outbound session request regarding a different target set, we
            // need to re-establish it with another session id, otherwise it would get lost
            if session.target_set() != *target_set {
                self.initiate_session(remote_peer, target_set, &session.mode())?;
                // @TODO: Again, the new session will generate a message
                // which we send onto the swarm
            }
        }

        Ok(())
    }

    fn handle_sync_request(
        &mut self,
        remote_peer: &P,
        mode: &Mode,
        session_id: &SessionId,
        target_set: &TargetSet,
    ) -> Result<(), ReplicationError> {
        SyncManager::<P>::is_mode_supported(mode)?;

        let sessions = self.get_sessions(remote_peer);

        // Check if a session with this id already exists for this peer, this can happen if both
        // peers started to initiate a session at the same time, we can try to resolve this
        if let Some((index, session)) = sessions
            .iter()
            .enumerate()
            .find(|(_, session)| session.id == *session_id && session.local)
        {
            return self.handle_duplicate_session(remote_peer, target_set, index, session);
        }

        // Check if a session with this target set already exists for this peer, this always gets
        // rejected because it is clearly redundant
        if let Some(session) = sessions
            .iter()
            .find(|session| session.target_set() == *target_set)
        {
            return Err(ReplicationError::DuplicateInboundRequest(session.id));
        }

        self.insert_session(remote_peer, session_id, target_set, mode, false);

        Ok(())
    }

    pub fn handle_message(
        &mut self,
        remote_peer: &P,
        sync_message: &SyncMessage,
    ) -> Result<(), ReplicationError> {
        match sync_message.message() {
            Message::SyncRequest(mode, target_set) => {
                self.handle_sync_request(remote_peer, mode, &sync_message.session_id(), target_set)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::schema::{SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;

    use crate::replication::errors::ReplicationError;
    use crate::replication::message::Message;
    use crate::replication::{Mode, SyncMessage, TargetSet};

    use super::{SyncManager, INITIAL_SESSION_ID};

    const PEER_ID_LOCAL: &'static str = "local";
    const PEER_ID_REMOTE: &'static str = "remote";

    #[rstest::fixture]
    fn random_target_set() -> TargetSet {
        let document_view_id = random_document_view_id();
        let schema_id =
            SchemaId::new_application(&SchemaName::new("messages").unwrap(), &document_view_id);
        TargetSet::new(&[schema_id])
    }

    #[rstest]
    fn initiate_outbound_session(
        #[from(random_target_set)] target_set_1: TargetSet,
        #[from(random_target_set)] target_set_2: TargetSet,
    ) {
        let mode = Mode::Naive;

        let mut manager = SyncManager::new(PEER_ID_LOCAL);
        let result = manager.initiate_session(&PEER_ID_REMOTE, &target_set_1, &mode);
        assert!(result.is_ok());

        let result = manager.initiate_session(&PEER_ID_REMOTE, &target_set_2, &mode);
        assert!(result.is_ok());

        // Expect error when initiating a session for the same target set
        let result = manager.initiate_session(&PEER_ID_REMOTE, &target_set_1, &mode);
        assert!(matches!(
            result,
            Err(ReplicationError::DuplicateOutboundRequest(0))
        ));
    }

    #[rstest]
    fn initiate_inbound_session(
        #[from(random_target_set)] target_set_1: TargetSet,
        #[from(random_target_set)] target_set_2: TargetSet,
    ) {
        let mut manager = SyncManager::new(PEER_ID_LOCAL);

        let message = SyncMessage::new(0, Message::SyncRequest(Mode::Naive, target_set_1.clone()));
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_ok());

        let message = SyncMessage::new(1, Message::SyncRequest(Mode::Naive, target_set_2.clone()));
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_ok());

        // Reject attempt to create session again
        let message = SyncMessage::new(0, Message::SyncRequest(Mode::Naive, target_set_1.clone()));
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(matches!(
            result,
            Err(ReplicationError::DuplicateInboundRequest(0))
        ));

        // Reject different session concerning same target set
        let message = SyncMessage::new(2, Message::SyncRequest(Mode::Naive, target_set_2.clone()));
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(matches!(
            result,
            Err(ReplicationError::DuplicateInboundRequest(1))
        ));
    }

    #[rstest]
    fn inbound_checks_supported_mode(#[from(random_target_set)] target_set: TargetSet) {
        // Should not fail when requesting supported replication mode
        let mut manager = SyncManager::new(PEER_ID_LOCAL);
        let message = SyncMessage::new(
            INITIAL_SESSION_ID,
            Message::SyncRequest(Mode::Naive, target_set.clone()),
        );
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_ok());

        // Should fail when requesting unsupported replication mode
        let mut manager = SyncManager::new(PEER_ID_LOCAL);
        let message = SyncMessage::new(
            INITIAL_SESSION_ID,
            Message::SyncRequest(Mode::SetReconciliation, target_set.clone()),
        );
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_err());
    }
}
