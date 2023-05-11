// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::replication::TargetSet;

#[derive(Debug)]
pub enum SyncMessage {
    SyncRequest(u64, u64, TargetSet),
    Other,
}
