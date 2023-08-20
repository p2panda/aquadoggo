// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use crate::replication::TargetSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    /// This contains a list of schema ids this peer is interested in.
    target_set: TargetSet,

    /// Timestamp of this announcement. Helps to understand if we can override the previous
    /// announcement with a newer one.
    timestamp: u64,
}

impl Announcement {
    pub fn new(target_set: TargetSet) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time invalid, operation system time configured before UNIX epoch")
            .as_secs();

        Self {
            timestamp,
            target_set,
        }
    }
}
