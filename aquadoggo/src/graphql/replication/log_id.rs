// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;
use p2panda_rs::entry::LogId as PandaLogId;
use serde::{Deserialize, Serialize};

/// The log id of a bamboo entry
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LogId(pub PandaLogId);

impl LogId {
    #[allow(dead_code)]
    pub fn as_u64(&self) -> u64 {
        self.0.as_u64()
    }
}

impl From<u64> for LogId {
    fn from(n: u64) -> Self {
        Self(PandaLogId::new(n))
    }
}

impl AsRef<PandaLogId> for LogId {
    fn as_ref(&self) -> &PandaLogId {
        &self.0
    }
}

scalar!(LogId);
