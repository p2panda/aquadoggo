// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::hash::Hash;

use anyhow::Result;
use log::{debug, trace, warn};
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::Human;

use crate::db::SqlStore;
use crate::replication::errors::{DuplicateSessionRequestError, IngestError, ReplicationError};
use crate::replication::{
    Message, Mode, SchemaIdSet, Session, SessionId, SessionState, SyncIngest, SyncMessage,
};

pub const INITIAL_SESSION_ID: SessionId = 0;

pub const SUPPORTED_MODES: [Mode; 1] = [Mode::LogHeight];

pub const SUPPORT_LIVE_MODE: bool = false;

fn to_sync_messages(session_id: SessionId, messages: Vec<Message>) -> Vec<SyncMessage> {
    messages
        .into_iter()
        .map(|message| SyncMessage::new(session_id, message))
        .collect()
}

#[derive(Clone, Debug)]
pub struct SyncResult {
    pub messages: Vec<SyncMessage>,
    pub is_done: bool,
}

impl SyncResult {
    pub fn from_messages(session_id: SessionId, messages: Vec<Message>, is_done: bool) -> Self {
        Self {
            is_done,
            messages: to_sync_messages(session_id, messages),
        }
    }
}

#[derive(Debug)]
pub struct SyncManager<P> {
    store: SqlStore,
    ingest: SyncIngest,
    local_peer: P,
    sessions: HashMap<P, Vec<Session>>,
}

impl<P> SyncManager<P>
where
    P: Clone + Human + Hash + Eq + PartialOrd,
{
    pub fn new(store: SqlStore, ingest: SyncIngest, local_peer: P) -> Self {
        Self {
            store,
            local_peer,
            ingest,
            sessions: HashMap::new(),
        }
    }

    /// Removes all sessions related to a remote peer.
    ///
    /// Warning: This might also remove actively running sessions. Do only clear sessions when you
    /// are sure they are a) done or b) the peer closed its connection.
    pub fn remove_sessions(&mut self, remote_peer: &P) {
        self.sessions.remove(remote_peer);
    }

    /// Get all sessions related to a remote peer.
    pub fn get_sessions(&self, remote_peer: &P) -> Vec<Session> {
        self.sessions
            .get(remote_peer)
            // Always return an array, even when it is empty
            .unwrap_or(&vec![])
            .to_owned()
    }

    /// Register a new session in manager and retrieve initial messages from it.
    async fn insert_and_initialize_session(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        target_set: &SchemaIdSet,
        mode: &Mode,
        local: bool,
    ) -> Vec<Message> {
        let mut session = Session::new(
            session_id,
            target_set,
            mode,
            local,
            SUPPORT_LIVE_MODE,
            self.ingest.schema_provider.clone(),
        );
        let initial_messages = session.initial_messages(&self.store).await;

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            self.sessions.insert(remote_peer.clone(), vec![session]);
        }

        initial_messages
    }

    /// Register a new session in manager.
    async fn insert_session(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        target_set: &SchemaIdSet,
        mode: &Mode,
        local: bool,
    ) {
        let session = Session::new(
            session_id,
            target_set,
            mode,
            local,
            SUPPORT_LIVE_MODE,
            self.ingest.schema_provider.clone(),
        );

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            self.sessions.insert(remote_peer.clone(), vec![session]);
        }
    }

    pub fn remove_session(&mut self, remote_peer: &P, session_id: &SessionId) {
        let sessions = self.sessions.get_mut(remote_peer);

        if let Some(sessions) = sessions {
            if let Some((index, _)) = sessions
                .iter()
                .enumerate()
                .find(|(_, session)| session.id == *session_id)
            {
                sessions.remove(index);
            } else {
                debug!(
                    "Tried to remove nonexistent session {} with peer: {}",
                    session_id,
                    remote_peer.display()
                );
            }
        } else {
            warn!(
                "Tried to remove sessions from unknown peer: {}",
                remote_peer.display()
            );
        }
    }

    // @TODO: Make error type smaller in size
    #[allow(clippy::result_large_err)]
    fn is_mode_supported(mode: &Mode) -> Result<(), ReplicationError> {
        if !SUPPORTED_MODES.contains(mode) {
            return Err(ReplicationError::UnsupportedMode);
        }

        Ok(())
    }

    pub async fn initiate_session(
        &mut self,
        remote_peer: &P,
        target_set: &SchemaIdSet,
        mode: &Mode,
    ) -> Result<Vec<SyncMessage>, ReplicationError> {
        SyncManager::<P>::is_mode_supported(mode)?;

        let sessions = self.get_sessions(remote_peer);

        debug!(
            "Initiate outbound replication session with peer {}",
            remote_peer.display()
        );

        // Make sure to not have duplicate sessions over the same schema ids
        let session = sessions
            .iter()
            .find(|session| session.target_set() == *target_set);

        match session {
            Some(session) => Err(DuplicateSessionRequestError::OutboundExistingTargetSet(
                session.target_set(),
            )),
            None => Ok(()),
        }?;

        // Determine next id for upcoming session
        let session_id = {
            if let Some(session) = sessions.last() {
                session.id + 1
            } else {
                INITIAL_SESSION_ID
            }
        };

        // Ignore initial messages when we initiated the session, they will come from the other
        // peer
        self.insert_session(remote_peer, &session_id, target_set, mode, true)
            .await;

        Ok(vec![SyncMessage::new(
            session_id,
            Message::SyncRequest(mode.clone(), target_set.clone()),
        )])
    }

    async fn handle_duplicate_session(
        &mut self,
        remote_peer: &P,
        target_set: &SchemaIdSet,
        existing_session: &Session,
    ) -> Result<SyncResult, ReplicationError> {
        match existing_session.local {
            // Remote peer sent a sync request for an already pending inbound session, we should
            // ignore this second request.
            false => Err(DuplicateSessionRequestError::InboundPendingSession(
                existing_session.id,
            )),
            _ => Ok(()),
        }?;

        let accept_inbound_request = match existing_session.state {
            // Handle only duplicate sessions when they haven't started yet
            SessionState::Pending => {
                if &self.local_peer < remote_peer {
                    // Drop our pending session
                    debug!(
                        "Drop pending outbound session and process inbound session request with duplicate id {}",
                        existing_session.id
                    );
                    self.remove_session(remote_peer, &existing_session.id);

                    // Accept the inbound request
                    Ok(true)
                } else {
                    // Keep our pending session, ignore inbound request
                    debug!(
                        "Ignore inbound request and keep pending outbound session with duplicate id {}",
                        existing_session.id
                    );
                    Ok(false)
                }
            }
            SessionState::Established => Err(
                DuplicateSessionRequestError::InboundEstablishedSession(existing_session.id),
            ),
            SessionState::Done => Err(DuplicateSessionRequestError::InboundDoneSession(
                existing_session.id,
            )),
        }?;

        let mut all_messages: Vec<SyncMessage> = vec![];

        if accept_inbound_request {
            let messages = self
                .insert_and_initialize_session(
                    remote_peer,
                    &existing_session.id,
                    target_set,
                    &existing_session.mode(),
                    false,
                )
                .await;
            all_messages.extend(to_sync_messages(existing_session.id, messages));

            // If we dropped our own outbound session request regarding a different target set, we
            // need to re-establish it with another session id, otherwise it would get lost
            if existing_session.target_set() != *target_set {
                let messages = self
                    .initiate_session(
                        remote_peer,
                        &existing_session.target_set(),
                        &existing_session.mode(),
                    )
                    .await?;
                all_messages.extend(messages)
            }
        }

        Ok(SyncResult {
            messages: all_messages,
            is_done: false,
        })
    }

    async fn handle_duplicate_target_set(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        mode: &Mode,
        existing_session: &Session,
    ) -> Result<SyncResult, ReplicationError> {
        match existing_session.local {
            // Remote peer sent a sync request for an already pending inbound session, we should
            // ignore this second request.
            false => Err(DuplicateSessionRequestError::InboundExistingTargetSet(
                existing_session.target_set(),
            )),
            _ => Ok(()),
        }?;

        let accept_inbound_request = match existing_session.state {
            // Handle only duplicate sessions when they haven't started yet
            SessionState::Pending => {
                if &self.local_peer < remote_peer {
                    // Drop our pending session
                    debug!(
                        "Drop pending outbound session and process inbound session request with duplicate target set"
                    );
                    self.remove_session(remote_peer, &existing_session.id);

                    // Accept the inbound request
                    Ok(true)
                } else {
                    // Keep our pending session, ignore inbound request
                    debug!(
                        "Ignore inbound request and keep pending outbound session with duplicate target set",
                    );
                    Ok(false)
                }
            }
            _ => Err(DuplicateSessionRequestError::InboundExistingTargetSet(
                existing_session.target_set(),
            )),
        }?;

        let mut all_messages: Vec<SyncMessage> = vec![];

        if accept_inbound_request {
            let messages = self
                .insert_and_initialize_session(
                    remote_peer,
                    session_id,
                    &existing_session.target_set(),
                    mode,
                    false,
                )
                .await;
            all_messages.extend(to_sync_messages(existing_session.id, messages));
        }

        Ok(SyncResult {
            messages: all_messages,
            is_done: false,
        })
    }

    async fn handle_sync_request(
        &mut self,
        remote_peer: &P,
        mode: &Mode,
        session_id: &SessionId,
        target_set: &SchemaIdSet,
    ) -> Result<SyncResult, ReplicationError> {
        SyncManager::<P>::is_mode_supported(mode)?;

        let sessions = self.get_sessions(remote_peer);

        // Check if a session with this id already exists for this peer.
        //
        // This can happen if both peers started to initiate a session at the same time, or if the
        // remote peer sent two sync request messages with the same session id.
        if let Some(existing_session) = sessions
            .iter()
            .find(|existing_session| existing_session.id == *session_id)
        {
            trace!("Handle sync request containing duplicate session id");
            return self
                .handle_duplicate_session(remote_peer, target_set, existing_session)
                .await;
        }

        // Check if a session with this target set already exists for this peer.
        if let Some(session) = sessions
            .iter()
            .find(|session| session.target_set() == *target_set)
        {
            trace!("Handle sync request containing duplicate target sets");
            return self
                .handle_duplicate_target_set(remote_peer, session_id, mode, session)
                .await;
        };

        debug!(
            "Accept inbound replication session with peer {}",
            remote_peer.display()
        );

        let messages = self
            .insert_and_initialize_session(remote_peer, session_id, target_set, mode, false)
            .await;

        Ok(SyncResult::from_messages(*session_id, messages, false))
    }

    async fn handle_session_message(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        message: &Message,
    ) -> Result<SyncResult, ReplicationError> {
        trace!(
            "Message received: {} {} {}",
            session_id,
            remote_peer.display(),
            message.display(),
        );

        let sessions = self.sessions.get_mut(remote_peer);

        let (is_both_done, messages) = match sessions {
            Some(sessions) => {
                // Check if a session exists with the given id for this peer.
                if let Some(session) = sessions
                    .iter_mut()
                    .find(|session| session.id == *session_id)
                {
                    // Pass the message onto the session when found.
                    let messages = session.handle_message(&self.store, message).await?;
                    let is_both_done = session.state == SessionState::Done;
                    Ok((is_both_done, messages))
                } else {
                    Err(ReplicationError::NoSessionFound(
                        *session_id,
                        remote_peer.display(),
                    ))
                }
            }
            None => Err(ReplicationError::NoPeerFound(remote_peer.display())),
        }?;

        // We're done, clean up after ourselves
        if is_both_done {
            self.remove_session(remote_peer, session_id);
        }

        Ok(SyncResult::from_messages(
            *session_id,
            messages,
            is_both_done,
        ))
    }

    async fn handle_entry(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        entry_bytes: &EncodedEntry,
        operation_bytes: &Option<EncodedOperation>,
    ) -> Result<SyncResult, ReplicationError> {
        let mut sessions = self.get_sessions(remote_peer);

        if let Some(session) = sessions
            .iter_mut()
            .find(|session| session.id == *session_id)
        {
            session.validate_entry(entry_bytes, operation_bytes.as_ref())?;

            match self
                .ingest
                .handle_entry(
                    &self.store,
                    entry_bytes,
                    // @TODO: This should handle entries with removed payloads
                    operation_bytes
                        .as_ref()
                        .expect("For now we always expect an operation here"),
                )
                .await
            {
                // When duplicate entries arrive at a node, or a schema is not materialized yet,
                // we don't want to treat as an error. This is expected behavior which may occur
                // when concurrent sync sessions are running.
                Ok(_) | Err(IngestError::DuplicateEntry(_)) | Err(IngestError::SchemaNotFound) => {
                    Ok(SyncResult {
                        messages: vec![],
                        is_done: session.state == SessionState::Done,
                    })
                }
                Err(err) => Err(ReplicationError::Validation(err)),
            }
        } else {
            Err(ReplicationError::NoSessionFound(
                *session_id,
                remote_peer.display(),
            ))
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
            Message::Entry(entry_bytes, operation_bytes) => {
                self.handle_entry(
                    remote_peer,
                    &sync_message.session_id(),
                    entry_bytes,
                    operation_bytes,
                )
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
    use p2panda_rs::Human;
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::replication::errors::{DuplicateSessionRequestError, ReplicationError};
    use crate::replication::message::Message;
    use crate::replication::{
        Mode, SchemaIdSet, SyncIngest, SyncMessage, HAVE_TYPE, SYNC_DONE_TYPE,
    };
    use crate::schema::SchemaProvider;
    use crate::test_utils::helpers::random_schema_id_set;
    use crate::test_utils::{
        generate_key_pairs, populate_and_materialize, populate_store_config, test_runner,
        test_runner_with_manager, PopulateStoreConfig, TestNode, TestNodeManager,
    };

    use super::{SyncManager, INITIAL_SESSION_ID};

    #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct Peer(String);

    impl Peer {
        pub fn new(id: &str) -> Self {
            Self(id.to_string())
        }
    }

    impl Human for Peer {
        fn display(&self) -> String {
            self.0.clone()
        }
    }

    #[rstest]
    fn initiate_outbound_session(
        #[from(random_schema_id_set)] target_set_1: SchemaIdSet,
        #[from(random_schema_id_set)] target_set_2: SchemaIdSet,
    ) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner(move |node: TestNode| async move {
            let mode = Mode::LogHeight;
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            let mut manager = SyncManager::new(node.context.store.clone(), ingest, peer_id_local);
            let result = manager
                .initiate_session(&peer_id_remote, &target_set_1, &mode)
                .await;
            assert!(result.is_ok());

            let result = manager
                .initiate_session(&peer_id_remote, &target_set_2, &mode)
                .await;
            assert!(result.is_ok());

            // Expect error when initiating a session for the same target set
            let result = manager
                .initiate_session(&peer_id_remote, &target_set_1, &mode)
                .await;
            assert!(matches!(
                result,
                Err(ReplicationError::DuplicateSession(err)) if err == DuplicateSessionRequestError::OutboundExistingTargetSet(target_set_1)
            ));
        })
    }

    #[rstest]
    fn initiate_inbound_session(
        #[from(random_schema_id_set)] target_set_1: SchemaIdSet,
        #[from(random_schema_id_set)] target_set_2: SchemaIdSet,
        #[from(random_schema_id_set)] target_set_3: SchemaIdSet,
    ) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner(move |node: TestNode| async move {
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            let mut manager = SyncManager::new(node.context.store.clone(), ingest, peer_id_local);

            let message = SyncMessage::new(
                0,
                Message::SyncRequest(Mode::LogHeight, target_set_1.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(result.is_ok());

            let message = SyncMessage::new(
                1,
                Message::SyncRequest(Mode::LogHeight, target_set_2.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(result.is_ok());

            // Reject attempt to create session again
            let message = SyncMessage::new(
                0,
                Message::SyncRequest(Mode::LogHeight, target_set_3.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(matches!(result,
                Err(ReplicationError::DuplicateSession(err)) if err == DuplicateSessionRequestError::InboundPendingSession(0)
            ));

            // Reject different session concerning same target set
            let message = SyncMessage::new(
                2,
                Message::SyncRequest(Mode::LogHeight, target_set_2.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(matches!(
                result,
                Err(ReplicationError::DuplicateSession(err)) if err == DuplicateSessionRequestError::InboundExistingTargetSet(target_set_2)
            ));
        })
    }

    //  PEER A                                 PEER B
    //
    //  SyncRequest(0, 0, ["A"])────────────────────►
    //
    //  ◄─────────────────── SyncRequest(0, 0, ["B"])
    //
    //  ========== PEER A REQUEST DROPPED ===========
    //
    //  0 Have([..]) ───────────────────────────────►
    //
    //  0 SyncDone(false) ─────┐
    //                         │
    //  ◄──────────────────────┼──────── 0 Have([..])
    //                         │
    //  ◄──────────────────────┼─── 0 SyncDone(false)
    //                         │
    //                         └────────────────────►
    //
    //  ============ SESSION 0 CLOSED ===============
    //
    //  ====== PEER A REPEATS WITH NEW SESS ID ======
    //
    //  SyncRequest(1, 0, ["A"])────────────────────►
    //
    //  ◄─────────────────────────────── 1 Have([..])
    //
    //                         ┌─── 1 SyncDone(false)
    //                         │
    //  1 Have([..]) ──────────┼────────────────────►
    //                         │
    //  1 SyncDone(false) ─────┼────────────────────►
    //                         │
    //  ◄──────────────────────┘
    //
    //  ============ SESSION 1 CLOSED ===============
    #[rstest]
    fn concurrent_requests_duplicate_session_ids(
        #[from(random_schema_id_set)] target_set_1: SchemaIdSet,
        #[from(random_schema_id_set)] target_set_2: SchemaIdSet,
    ) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner(move |node: TestNode| async move {
            let mode = Mode::LogHeight;
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            // Sanity check: Id of peer A is < id of peer B.
            //
            // This is important for testing the deterministic handling of concurrent session
            // requests which contain the same session id.
            assert!(peer_id_local < peer_id_remote);

            // Sanity check: Target sets need to be different
            assert!(target_set_1 != target_set_2);

            // Local peer A initiates a session with id 0 and target set 1.
            let mut manager_a = SyncManager::new(
                node.context.store.clone(),
                ingest.clone(),
                peer_id_local.clone(),
            );
            let result = manager_a
                .initiate_session(&peer_id_remote, &target_set_1, &mode)
                .await
                .unwrap();

            assert_eq!(result.len(), 1);
            let sync_request_a = result[0].clone();

            // Remote peer B initiates a session with id 0 and target set 2.
            //
            // Note that both peers use the _same_ session id but _different_ target sets.
            let mut manager_b =
                SyncManager::new(node.context.store.clone(), ingest, peer_id_remote.clone());
            let result = manager_b
                .initiate_session(&peer_id_local, &target_set_2, &mode)
                .await
                .unwrap();

            assert_eq!(result.len(), 1);
            let sync_request_b = result[0].clone();

            // Both peers send and handle the requests concurrently.
            let result = manager_a
                .handle_message(&peer_id_remote, &sync_request_b)
                .await
                .unwrap();

            // We expect Peer A to:
            //
            // 1. Drop their pending outgoing session
            // 2. Respond to the request from Peer B
            // 3. Send another sync request for the other target set with a corrected session id
            assert_eq!(result.messages.len(), 3);
            let (have_message_a, done_message_a, sync_request_a_corrected) = (
                result.messages[0].clone(),
                result.messages[1].clone(),
                result.messages[2].clone(),
            );

            let result = manager_b
                .handle_message(&peer_id_local, &sync_request_a)
                .await
                .unwrap();

            // We expect Peer B to drop the incoming request from Peer A and simply wait for a
            // response from its original request
            assert_eq!(result.messages.len(), 0);

            // Peer A has two sessions running: The one initiated by Peer B and the one it
            // re-initiated itself with the new session id
            let manager_a_sessions = manager_a.get_sessions(&peer_id_remote);
            assert_eq!(manager_a_sessions.len(), 2);

            // Peer B has still one running, it didn't learn about the re-initiated session of A
            // yet
            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 1);

            // Peer B processes the `Have`, `SyncDone` and `SyncRequest` messages from Peer A.
            let result = manager_b
                .handle_message(&peer_id_local, &have_message_a)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 2);

            // They send their own `Have` and `SyncDone` messages.
            let (have_message_b, done_message_b) =
                (response.messages[0].clone(), response.messages[1].clone());

            // Sync done, they send no more messages.
            let result = manager_b
                .handle_message(&peer_id_local, &done_message_a)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // Peer B should have closed the session for good
            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 0);

            // Now the second, re-established sync request from peer A concerning another target
            // set arrives at peer B
            let result = manager_b
                .handle_message(&peer_id_local, &sync_request_a_corrected)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 2);

            // They send their own `Have` and `SyncDone` messages for the corrected target set
            let (have_message_b_corrected, done_message_b_corrected) =
                (response.messages[0].clone(), response.messages[1].clone());

            // Peer B should now know about one session again
            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 1);

            // Peer A processes both the `Have` and `SyncDone` messages from Peer B for the first
            // session and produces no new messages. We're done with this session on Peer A as
            // well now.
            let result = manager_a
                .handle_message(&peer_id_remote, &have_message_b)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            let result = manager_a
                .handle_message(&peer_id_remote, &done_message_b)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // Peer A should now know about one session again
            let manager_a_sessions = manager_a.get_sessions(&peer_id_remote);
            assert_eq!(manager_a_sessions.len(), 1);

            // Peer A processes both the re-initiated sessions `Have` and `SyncDone` messages from
            // Peer B and produces its own answer.
            let result = manager_a
                .handle_message(&peer_id_remote, &have_message_b_corrected)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 2);

            let (have_message_a_corrected, done_message_a_corrected) =
                (response.messages[0].clone(), response.messages[1].clone());

            let result = manager_a
                .handle_message(&peer_id_remote, &done_message_b_corrected)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // Peer B processes both the re-initiated `Have` and `SyncDone` messages from Peer A
            // and produces no new messages.
            let result = manager_b
                .handle_message(&peer_id_local, &have_message_a_corrected)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            let result = manager_b
                .handle_message(&peer_id_local, &done_message_a_corrected)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // After processing all messages both peers should have no sessions remaining.
            let manager_a_sessions = manager_a.get_sessions(&peer_id_remote);
            assert_eq!(manager_a_sessions.len(), 0);

            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 0);
        })
    }

    //  PEER A                                 PEER B
    //
    //  SyncRequest(0, 0, ["A"])────────────────────►
    //
    //  ◄─────────────────── SyncRequest(0, 1, ["A"])
    //
    //  ========== PEER A REQUEST DROPPED ===========
    //
    //  Have([..]) ─────────────────────────────────►
    //
    //  Done(false) ───────────┐
    //                         │
    //  ◄──────────────────────┼────────── Have([..])
    //                         │
    //  ◄──────────────────────┼───────── Done(false)
    //                         │
    //                         └────────────────────►
    //
    //  ============== SESSION CLOSED ===============
    #[rstest]
    fn concurrent_requests_duplicate_target_set(
        #[from(random_schema_id_set)] target_set_1: SchemaIdSet,
    ) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner(move |node: TestNode| async move {
            let mode = Mode::LogHeight;
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            // Local peer id is < than remote, this is important for testing the deterministic
            // handling of concurrent session requests which contain the same session id.
            assert!(peer_id_local < peer_id_remote);

            let mut manager_a = SyncManager::new(
                node.context.store.clone(),
                ingest.clone(),
                peer_id_local.clone(),
            );

            let mut manager_b =
                SyncManager::new(node.context.store.clone(), ingest, peer_id_remote.clone());

            // Local peer A initiates a session with target set A.
            let result = manager_a
                .initiate_session(&peer_id_remote, &target_set_1, &mode)
                .await;

            let sync_messages = result.unwrap();
            assert_eq!(sync_messages.len(), 1);
            let sync_request_a = sync_messages[0].clone();

            // Remote peer B initiates a session with a dummy peer just to increment the session
            // id.
            let dummy_peer_id = Peer::new("some_other_peer");
            let _result = manager_b
                .initiate_session(&dummy_peer_id, &target_set_1, &mode)
                .await;

            // Remote peer B initiates a session with target set A.
            let result = manager_b
                .initiate_session(&peer_id_local, &target_set_1, &mode)
                .await;

            let sync_messages = result.unwrap();
            assert_eq!(sync_messages.len(), 1);
            let sync_request_b = sync_messages[0].clone();

            // Remove the session from the dummy peer.
            manager_b.remove_sessions(&dummy_peer_id);

            // Both peers send and handle the requests concurrently.
            let result = manager_a
                .handle_message(&peer_id_remote, &sync_request_b)
                .await;
            let response = result.unwrap();

            // We expect Peer A to drop their pending outgoing session and respond to the request
            // from Peer B.
            assert_eq!(response.messages.len(), 2);
            let (have_message_a, done_message_a) =
                (response.messages[0].clone(), response.messages[1].clone());

            let result = manager_b
                .handle_message(&peer_id_local, &sync_request_a)
                .await;
            let response = result.unwrap();

            // We expect Peer B to drop the incoming request from Peer A and simply wait
            // for a response from its original request
            assert_eq!(response.messages.len(), 0);

            // Both peers have exactly one session running.
            let manager_a_sessions = manager_a.get_sessions(&peer_id_remote);
            assert_eq!(manager_a_sessions.len(), 1);

            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 1);

            // Peer B processes the `Have` and `SyncDone` messages from Peer A.
            let result = manager_b
                .handle_message(&peer_id_local, &have_message_a)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 2);

            // They send their own `Have` and `SyncDone` messages.
            let (have_message_b, done_message_b) =
                (response.messages[0].clone(), response.messages[1].clone());

            // Sync done, they send no more messages.
            let result = manager_b
                .handle_message(&peer_id_local, &done_message_a)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // Peer A processes both the `Have` and `SyncDone` messages from Peer B and produces
            // no new messages.
            let result = manager_a
                .handle_message(&peer_id_remote, &have_message_b)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            let result = manager_a
                .handle_message(&peer_id_remote, &done_message_b)
                .await;
            let response = result.unwrap();
            assert_eq!(response.messages.len(), 0);

            // After processing all messages both peers should have no sessions remaining.
            let manager_a_sessions = manager_a.get_sessions(&peer_id_remote);
            assert_eq!(manager_a_sessions.len(), 0);

            let manager_b_sessions = manager_b.get_sessions(&peer_id_local);
            assert_eq!(manager_b_sessions.len(), 0);
        })
    }

    #[rstest]
    fn inbound_checks_supported_mode(#[from(random_schema_id_set)] target_set: SchemaIdSet) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner(move |node: TestNode| async move {
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            // Should not fail when requesting supported replication mode
            let mut manager = SyncManager::new(
                node.context.store.clone(),
                ingest.clone(),
                peer_id_local.clone(),
            );
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::LogHeight, target_set.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(result.is_ok());

            // Should fail when requesting unsupported replication mode
            let mut manager = SyncManager::new(node.context.store.clone(), ingest, peer_id_local);
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::SetReconciliation, target_set.clone()),
            );
            let result = manager.handle_message(&peer_id_remote, &message).await;
            assert!(result.is_err());
        })
    }

    // PEER A                               PEER B
    //
    // SyncRequest(0, 0, [..])─────────────────────►
    //
    // ◄───────────────────────────────── Have([..])
    //
    //                      ┌─────── SyncDone(false)
    //                      │
    // Have([..]) ──────────┼──────────────────────►
    //                      │
    // Entry(..)  ──────────┼──────────────────────►
    //                      │
    // Entry(..) ───────────┼──────────────────────►
    //                      │
    // Entry(..) ───────────┼──────────────────────►
    //                      │
    // Entry(..) ───────────┼──────────────────────►
    //                      │
    // Entry(..) ───────────┼──────────────────────►
    //                      │
    // Entry(..) ───────────┼──────────────────────►
    //                      │
    // SyncDone(false) ─────┼──────────────────────►
    //                      │
    // ◄────────────────────┘
    #[rstest]
    fn sync_lifetime(
        #[from(populate_store_config)]
        #[with(2, 1, generate_key_pairs(3))]
        config_a: PopulateStoreConfig,
        #[from(populate_store_config)] config_b: PopulateStoreConfig,
    ) {
        let peer_id_local: Peer = Peer::new("local");
        let peer_id_remote: Peer = Peer::new("remote");

        test_runner_with_manager(|manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let mut node_b = manager.create().await;

            populate_and_materialize(&mut node_a, &config_a).await;
            populate_and_materialize(&mut node_b, &config_b).await;

            let (tx, _rx) = broadcast::channel(8);
            let target_set = SchemaIdSet::new(&[config_a.schema.id().to_owned()]);

            let mut manager_a = SyncManager::new(
                node_a.context.store.clone(),
                SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
                peer_id_local.clone(),
            );

            let mut manager_b = SyncManager::new(
                node_b.context.store.clone(),
                SyncIngest::new(node_b.context.schema_provider.clone(), tx),
                peer_id_remote.clone(),
            );

            // Send `SyncRequest` to remote
            let messages = manager_a
                .initiate_session(&peer_id_remote, &target_set, &Mode::LogHeight)
                .await
                .unwrap();

            assert_eq!(
                messages,
                vec![SyncMessage::new(
                    0,
                    Message::SyncRequest(Mode::LogHeight, target_set.clone())
                )]
            );

            // Remote receives `SyncRequest`
            // Send `Have` and `SyncDone` messages back to local
            let result = manager_b
                .handle_message(&peer_id_local, &messages[0])
                .await
                .unwrap();

            assert!(!result.is_done);
            assert_eq!(
                result.messages,
                vec![
                    SyncMessage::new(0, Message::Have(vec![])),
                    SyncMessage::new(0, Message::SyncDone(false)),
                ]
            );

            // Receive `Have` and `SyncDone` messages from remote
            // Send `Have`, `Entry` and `SyncDone` messages to remote
            let result_have = manager_a
                .handle_message(&peer_id_remote, &result.messages[0])
                .await
                .unwrap();
            assert!(!result_have.is_done);

            let result_done = manager_a
                .handle_message(&peer_id_remote, &result.messages[1])
                .await
                .unwrap();
            assert!(result_done.is_done);

            assert_eq!(result_have.messages.len(), 8);
            assert_eq!(
                result_have.messages.first().unwrap().message_type(),
                HAVE_TYPE
            );
            assert_eq!(
                result_have.messages.last().unwrap().message_type(),
                SYNC_DONE_TYPE
            );

            // Remote receives `Have`, `Entry` `SyncDone` messages from local
            for (index, message) in result_have.messages.iter().enumerate() {
                let result = manager_b
                    .handle_message(&peer_id_local, message)
                    .await
                    .unwrap();

                // We're done when we received the last message (SyncDone)
                assert_eq!(result.is_done, index == result_have.messages.len() - 1);

                // We don't expect any messages anymore, Manager B didn't have any data
                assert!(result.messages.is_empty());
            }
        })
    }
}
