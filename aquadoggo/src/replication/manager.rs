// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::operation::EncodedOperation;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::{
    Message, Mode, Session, SessionId, SessionState, SyncIngest, SyncMessage, TargetSet,
};

pub const INITIAL_SESSION_ID: SessionId = 0;

pub const SUPPORTED_MODES: [Mode; 1] = [Mode::Naive];

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
    P: Clone + std::hash::Hash + Eq + PartialOrd,
{
    pub fn new(store: SqlStore, ingest: SyncIngest, local_peer: P) -> Self {
        Self {
            store,
            local_peer,
            ingest,
            sessions: HashMap::new(),
        }
    }

    pub fn remove_sessions(&mut self, remote_peer: &P) {
        self.sessions.remove(remote_peer);
    }

    /// Get all sessions related to a remote peer.
    fn get_sessions(&self, remote_peer: &P) -> Vec<Session> {
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

    /// Register a new session in manager.
    async fn insert_session(
        &mut self,
        remote_peer: &P,
        session_id: &SessionId,
        target_set: &TargetSet,
        mode: &Mode,
        local: bool,
    ) {
        let session = Session::new(session_id, target_set, mode, local, SUPPORT_LIVE_MODE);

        if let Some(sessions) = self.sessions.get_mut(remote_peer) {
            sessions.push(session);
        } else {
            self.sessions.insert(remote_peer.clone(), vec![session]);
        }
    }

    fn remove_session(&mut self, remote_peer: &P, session_id: &SessionId) {
        let sessions = self.sessions.get_mut(remote_peer);

        if let Some(sessions) = sessions {
            if let Some((index, _)) = sessions
                .iter()
                .enumerate()
                .find(|(_, session)| session.id == *session_id)
            {
                sessions.remove(index);
            }
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
        target_set: &TargetSet,
        mode: &Mode,
    ) -> Result<Vec<SyncMessage>, ReplicationError> {
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
        target_set: &TargetSet,
        session: &Session,
    ) -> Result<SyncResult, ReplicationError> {
        let accept_inbound_request = match session.state {
            // Handle only duplicate sessions when they haven't started yet
            SessionState::Pending => {
                if &self.local_peer < remote_peer {
                    // Drop our pending session
                    self.remove_session(remote_peer, &session.id);

                    // Accept the inbound request
                    true
                } else {
                    // Keep our pending session, ignore inbound request
                    false
                }
            }
            _ => return Err(ReplicationError::DuplicateInboundRequest(session.id)),
        };

        let mut all_messages: Vec<SyncMessage> = vec![];

        if accept_inbound_request {
            let messages = self
                .insert_and_initialize_session(
                    remote_peer,
                    &session.id,
                    target_set,
                    &session.mode(),
                    false,
                )
                .await;
            all_messages.extend(to_sync_messages(session.id, messages));

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
            messages: all_messages,
            is_done: false,
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
        if let Some(session) = sessions
            .iter()
            .find(|session| session.id == *session_id && session.local)
        {
            return self
                .handle_duplicate_session(remote_peer, target_set, session)
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
                    Err(ReplicationError::NoSessionFound(*session_id))
                }
            }
            None => Err(ReplicationError::NoSessionFound(*session_id)),
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

            self.ingest
                .handle_entry(
                    &self.store,
                    entry_bytes,
                    // @TODO: This should handle entries with removed payloads
                    operation_bytes
                        .as_ref()
                        .expect("For now we always expect an operation here"),
                )
                .await?;

            Ok(SyncResult {
                messages: vec![],
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
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::replication::errors::ReplicationError;
    use crate::replication::message::{Message, HAVE_TYPE, SYNC_DONE_TYPE};
    use crate::replication::{Mode, SyncIngest, SyncMessage, TargetSet};
    use crate::schema::SchemaProvider;
    use crate::test_utils::helpers::random_target_set;
    use crate::test_utils::{
        populate_and_materialize, populate_store_config, test_runner, test_runner_with_manager,
        TestNode, TestNodeManager,
    };

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
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            let mut manager = SyncManager::new(node.context.store.clone(), ingest, PEER_ID_LOCAL);
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
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            let mut manager = SyncManager::new(node.context.store.clone(), ingest, PEER_ID_LOCAL);

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
            let (tx, _rx) = broadcast::channel(8);
            let ingest = SyncIngest::new(SchemaProvider::default(), tx);

            // Should not fail when requesting supported replication mode
            let mut manager =
                SyncManager::new(node.context.store.clone(), ingest.clone(), PEER_ID_LOCAL);
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::Naive, target_set.clone()),
            );
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_ok());

            // Should fail when requesting unsupported replication mode
            let mut manager = SyncManager::new(node.context.store.clone(), ingest, PEER_ID_LOCAL);
            let message = SyncMessage::new(
                INITIAL_SESSION_ID,
                Message::SyncRequest(Mode::SetReconciliation, target_set.clone()),
            );
            let result = manager.handle_message(&PEER_ID_REMOTE, &message).await;
            assert!(result.is_err());
        })
    }

    #[rstest]
    fn sync_lifetime(
        #[from(populate_store_config)]
        #[with(2, 1, 3)]
        config_a: PopulateStoreConfig,
        #[from(populate_store_config)] config_b: PopulateStoreConfig,
    ) {
        test_runner_with_manager(|manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let mut node_b = manager.create().await;

            populate_and_materialize(&mut node_a, &config_a).await;
            populate_and_materialize(&mut node_b, &config_b).await;

            let (tx, _rx) = broadcast::channel(8);
            let target_set = TargetSet::new(&vec![config_a.schema.id().to_owned()]);

            let mut manager_a = SyncManager::new(
                node_a.context.store.clone(),
                SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
                PEER_ID_LOCAL,
            );

            let mut manager_b = SyncManager::new(
                node_b.context.store.clone(),
                SyncIngest::new(node_b.context.schema_provider.clone(), tx),
                PEER_ID_REMOTE,
            );

            // Send `SyncRequest` to remote
            let messages = manager_a
                .initiate_session(&PEER_ID_REMOTE, &target_set, &Mode::Naive)
                .await
                .unwrap();

            assert_eq!(
                messages,
                vec![SyncMessage::new(
                    0,
                    Message::SyncRequest(Mode::Naive, target_set.clone())
                )]
            );

            // Receive `Have` and `SyncDone` from remote
            let result = manager_b
                .handle_message(&PEER_ID_LOCAL, &messages[0])
                .await
                .unwrap();

            assert_eq!(result.is_done, false);
            assert_eq!(
                result.messages,
                vec![
                    SyncMessage::new(0, Message::Have(vec![])),
                    SyncMessage::new(0, Message::SyncDone(false)),
                ]
            );

            // Send `Have`, `Entry` and `SyncDone` messages to remote
            let result_have = manager_a
                .handle_message(&PEER_ID_REMOTE, &result.messages[0])
                .await
                .unwrap();
            assert_eq!(result_have.is_done, false);

            let result_done = manager_a
                .handle_message(&PEER_ID_REMOTE, &result.messages[1])
                .await
                .unwrap();
            assert_eq!(result_done.is_done, true);

            assert_eq!(result_have.messages.len(), 8);
            assert_eq!(
                result_have.messages.first().unwrap().message_type(),
                HAVE_TYPE
            );
            assert_eq!(
                result_have.messages.last().unwrap().message_type(),
                SYNC_DONE_TYPE
            );

            // Receive `SyncDone` from remote
            for (index, message) in result_have.messages.iter().enumerate() {
                let result = manager_b
                    .handle_message(&PEER_ID_LOCAL, &message)
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
