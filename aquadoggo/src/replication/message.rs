// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::replication::{Mode, SessionId, TargetSet};

#[derive(Debug)]
pub enum SyncMessage {
    SyncRequest(Mode, SessionId, TargetSet),
    Other,
}

#[derive(Clone, Debug)]
pub enum StrategyMessage {
    Have,
    Entry,
}
