// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;

use crate::replication::errors::ReplicationError;
use crate::replication::{Mode, Session, SessionId, SessionState, SyncMessage, TargetSet};

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
            .get(&remote_peer)
            // Always return an array, even when it is empty
            .unwrap_or(&vec![])
            .to_owned()
    }

    /// Register a new session in manager.
    fn insert_session(&mut self, remote_peer: &P, session_id: &SessionId, target_set: &TargetSet) {
        let session = Session::new(session_id, target_set);

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
    ) -> Result<(), ReplicationError> {
        let sessions = self.get_sessions(remote_peer);

        // Make sure to not have duplicate sessions over the same schema ids
        let session = sessions
            .iter()
            .find(|session| &session.target_set == target_set);

        if let Some(session) = session {
            return Err(ReplicationError::DuplicateSession(session.id));
        }

        // Determine next id for upcoming session
        let session_id = {
            if let Some(session) = sessions.last() {
                session.id + 1
            } else {
                INITIAL_SESSION_ID
            }
        };

        self.insert_session(remote_peer, &session_id, target_set);

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
                    let mut sessions = self.get_sessions(remote_peer);
                    sessions.remove(index);

                    // Accept the inbound request
                    true
                } else {
                    // Keep our pending session, ignore inbound request
                    false
                }
            }
            _ => return Err(ReplicationError::DuplicateSession(session.id)),
        };

        if accept_inbound_request {
            self.insert_session(remote_peer, &session.id, &target_set);

            // @TODO: Session needs to generate some messages on creation and
            // it will pass them back up to us to then forward onto
            // the swarm

            // If we dropped our own outbound session request regarding a different target set, we
            // need to re-establish it with another session id, otherwise it would get lost
            if &session.target_set != target_set {
                self.initiate_session(remote_peer, &target_set)?;
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
        SyncManager::<P>::is_mode_supported(&mode)?;

        let sessions = self.get_sessions(remote_peer);

        // Check if a session with this id already exists for this peer
        if let Some((index, session)) = sessions
            .iter()
            .enumerate()
            .find(|(_, session)| session.id == *session_id)
        {
            return self.handle_duplicate_session(remote_peer, target_set, index, session);
        }

        // Check if a session with this target set already exists for this peer
        if let Some(session) = sessions
            .iter()
            .find(|session| &session.target_set == target_set)
        {
            return Err(ReplicationError::DuplicateSession(session.id));
        }

        self.insert_session(remote_peer, &session_id, &target_set);

        Ok(())
    }

    pub fn handle_message(
        &mut self,
        remote_peer: &P,
        message: &SyncMessage,
    ) -> Result<(), ReplicationError> {
        match message {
            SyncMessage::SyncRequest(mode, session_id, target_set) => {
                return self.handle_sync_request(remote_peer, mode, session_id, target_set);
            }
            SyncMessage::Other => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::schema_id;
    use rstest::rstest;

    use crate::replication::{Mode, SyncMessage, TargetSet};

    use super::{SyncManager, INITIAL_SESSION_ID};

    const PEER_ID_LOCAL: &'static str = "local";
    const PEER_ID_REMOTE: &'static str = "remote";

    #[rstest]
    fn checks_supported_mode(schema_id: SchemaId) {
        let target_set = TargetSet::new(&[schema_id]);

        // Should not fail when requesting supported replication mode
        let mut manager = SyncManager::new(PEER_ID_LOCAL);
        let message = SyncMessage::SyncRequest(Mode::Naive, INITIAL_SESSION_ID, target_set.clone());
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_ok());

        // Should fail when requesting unsupported replication mode
        let mut manager = SyncManager::new(PEER_ID_LOCAL);
        let message =
            SyncMessage::SyncRequest(Mode::SetReconciliation, INITIAL_SESSION_ID, target_set);
        let result = manager.handle_message(&PEER_ID_REMOTE, &message);
        assert!(result.is_err());
    }
}
