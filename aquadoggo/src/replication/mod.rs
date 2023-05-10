// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::{bail, Result};
use p2panda_rs::schema::SchemaId;

const INITIAL_SESSION_ID: SessionId = 0;
const SUPPORTED_MODES: [u64; 1] = [0];

type SessionId = u64;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TargetSet(Vec<SchemaId>);

impl TargetSet {
    pub fn new(schema_ids: &[SchemaId]) -> Self {
        // Sort schema ids to compare target sets easily
        let mut schema_ids = schema_ids.to_vec();
        schema_ids.sort();

        Self(schema_ids)
    }
}

#[derive(Debug)]
pub enum SyncMessage {
    SyncRequest(u64, u64, TargetSet),
    Other,
}

#[derive(Clone, Debug)]
pub enum SessionState {
    Pending,
    Established,
    Done,
}

#[derive(Clone, Debug)]
pub struct Session {
    id: SessionId,
    target_set: TargetSet,
    state: SessionState,
}

impl Session {
    pub fn new(id: &SessionId, target_set: &TargetSet) -> Self {
        Session {
            id: id.clone(),
            state: SessionState::Pending,
            target_set: target_set.clone(),
        }
    }
}

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

    fn insert_session(&mut self, remote_peer: &P, session_id: &SessionId, target_set: &TargetSet) {
        let session = Session::new(session_id, target_set);

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            let sessions = vec![session];
            self.sessions.insert(remote_peer.clone(), sessions);
        }
    }

    fn get_sessions(&self, remote_peer: &P) -> Option<&Vec<Session>> {
        self.sessions.get(remote_peer)
    }

    pub fn initiate_session(&mut self, remote_peer: &P, target_set: &TargetSet) -> Result<()> {
        let session_id = if let Some(sessions) = self.sessions.get(&remote_peer) {
            // Make sure to not have duplicate sessions over the same schema ids
            let session = sessions
                .iter()
                .find(|session| &session.target_set == target_set);

            if session.is_some() {
                bail!("Session concerning same schema ids already exists");
            }

            if let Some(session) = sessions.last() {
                session.id + 1
            } else {
                INITIAL_SESSION_ID
            }
        } else {
            INITIAL_SESSION_ID
        };

        self.insert_session(remote_peer, &session_id, target_set);

        Ok(())
    }

    pub fn handle_message(&mut self, remote_peer: &P, message: &SyncMessage) -> Result<()> {
        // Handle `SyncRequest`
        // If message = SyncRequest, then check if a) session id doesn't exist yet b) we're
        // not already handling the same schema id's in a running session
        //
        // If session id exists, then check if we just initialised it, in that case use peer id as
        // tie-breaker to decide who continues

        match message {
            SyncMessage::SyncRequest(mode, session_id, target_set) => {
                // Check we support this replication mode
                if !SUPPORTED_MODES.contains(&mode) {
                    bail!("Unsupported replication mode requested")
                }

                // Check if any sessions for this peer already exist
                if let Some(sessions) = self.sessions.get_mut(remote_peer) {
                    // lolz...
                    let sessions_clone = sessions.clone();

                    // Check if a session with this session id already exists for this peer
                    let session = sessions_clone
                        .iter()
                        .enumerate()
                        .find(|(_, session)| session.id == *session_id);

                    match session {
                        Some((index, session)) => {
                            // Check session state, if it is already established then this is an error
                            // if it is in initial state, then we need to compare peer ids and drop
                            // the connection depending on which is "lower".
                            let accept_sync_request = match session.state {
                                SessionState::Pending => {
                                    // Use peer ids as tie breaker as this SyncRequest contains a
                                    // existing sessin id.
                                    if &self.local_peer < remote_peer {
                                        // Drop our pending session
                                        sessions.remove(index);

                                        // We want to accept the incoming session
                                        true
                                    } else {
                                        // Keep our pending session
                                        // Ignore incoming sync request
                                        false
                                    }
                                }
                                _ => {
                                    bail!("Invalid SyncRequest received: duplicate session id used")
                                }
                            };

                            if accept_sync_request {
                                // Accept incoming sync request
                                self.insert_session(remote_peer, &session_id, &target_set);
                                // @TODO: Session needs to generate some messages on creation and
                                // it will pass them back up to us to then forward onto
                                // the swarm

                                // Check if the target sets match
                                if &session.target_set != target_set {
                                    // Send a new sync request for the dropped session
                                    // with new session id

                                    self.initiate_session(remote_peer, &target_set)?;
                                    // @TODO: Again, the new session will generate a message
                                    // which we send onto the swarm
                                }
                            }

                            // We handled the request we return here.
                            return Ok(());
                        }
                        // No duplicate session ids exist, move on.
                        None => (),
                    }

                    // Check we don't already have a session handling this target set
                    if sessions
                        .iter()
                        .find(|session| &session.target_set == target_set)
                        .is_some()
                    {
                        bail!("SyncRequest containing duplicate target set found")
                    }
                };

                self.insert_session(remote_peer, &session_id, &target_set);
                // @TODO: Handle messages that the new session will generate
            }
            SyncMessage::Other => todo!(),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
