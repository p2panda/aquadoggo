// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::replication::TargetSet;

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
    pub id: SessionId,
    pub target_set: TargetSet,
    pub state: SessionState,
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
