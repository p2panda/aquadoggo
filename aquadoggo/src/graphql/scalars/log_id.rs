// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Log id of a bamboo entry.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct LogId(p2panda_rs::entry::LogId);

impl From<p2panda_rs::entry::LogId> for LogId {
    fn from(log_id: p2panda_rs::entry::LogId) -> Self {
        Self(log_id)
    }
}

impl From<LogId> for p2panda_rs::entry::LogId {
    fn from(log_id: LogId) -> p2panda_rs::entry::LogId {
        log_id.0
    }
}

scalar!(LogId);
