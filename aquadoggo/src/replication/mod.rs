// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::{bail, Result};
use p2panda_rs::schema::SchemaId;

const INITIAL_SESSION_ID: SessionId = 0;

type SessionId = u64;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
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
pub enum SyncMessage {}

#[derive(Debug)]
pub struct Session {
    id: SessionId,
    target_set: TargetSet,
}

#[derive(Debug)]
pub struct SyncManager<P> {
    local_peer: P,
    sessions: HashMap<P, Vec<Session>>,
}

impl<P> SyncManager<P>
where
    P: std::hash::Hash + Eq,
{
    pub fn new(local_peer: P) -> Self {
        Self {
            local_peer,
            sessions: HashMap::new(),
        }
    }

    pub fn initiate_session(&self, remote_peer: P, schema_ids: &[SchemaId]) -> Result<()> {
        let target_set = TargetSet::new(schema_ids);

        let session_id = if let Some(sessions) = self.sessions.get(&remote_peer) {
            // Make sure to not have duplicate sessions over the same schema ids
            let session = sessions
                .iter()
                .find(|session| session.target_set == target_set);

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

        println!("{}", session_id);

        Ok(())
    }

    pub fn handle_message(&self, remote_peer: P, message: SyncMessage) -> Result<()> {
        // @TODO: Handle `SyncRequest`
        // @TODO: If message = SyncRequest, then check if a) session id doesn't exist yet b) we're
        // not already handling the same schema id's in a running session
        //
        // If session id exists, then check if we just initialised it, in that case use peer id as
        // tie-breaker to decide who continues

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
