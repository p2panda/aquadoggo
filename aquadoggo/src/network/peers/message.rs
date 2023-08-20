// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::{Deserialize, Serialize};

use crate::replication::{AnnouncementMessage, SyncMessage};

/// p2panda protocol messages which can be sent over the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PeerMessage {
    /// Announcement of peers about the schema ids they are interest in.
    Announce(AnnouncementMessage),

    /// Replication status and data exchange.
    SyncMessage(SyncMessage),
}
