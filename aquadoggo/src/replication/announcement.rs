// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use serde::ser::SerializeSeq;
use serde::Serialize;

use crate::replication::{SchemaIdSet, ANNOUNCE_TYPE, REPLICATION_PROTOCOL_VERSION};

/// U64 timestamp from UNIX epoch until now.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time invalid, operation system time configured before UNIX epoch")
        .as_secs()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    /// This contains a list of schema ids this peer allowed to support.
    pub supported_schema_ids: SchemaIdSet,

    /// Timestamp of this announcement. Helps to understand if we can override the previous
    /// announcement with a newer one.
    pub timestamp: u64,
}

impl Announcement {
    pub fn new(supported_schema_ids: SchemaIdSet) -> Self {
        Self {
            timestamp: now(),
            supported_schema_ids,
        }
    }
}

pub type ProtocolVersion = u64;

/// Message which can be used to send announcements over the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnouncementMessage(pub ProtocolVersion, pub Announcement);

impl AnnouncementMessage {
    pub fn new(announcement: Announcement) -> Self {
        Self(REPLICATION_PROTOCOL_VERSION, announcement)
    }

    pub fn announcement(&self) -> Announcement {
        self.1.clone()
    }

    pub fn is_version_supported(&self) -> bool {
        self.0 == REPLICATION_PROTOCOL_VERSION
    }
}

impl Serialize for AnnouncementMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(4))?;
        seq.serialize_element(&ANNOUNCE_TYPE)?;
        seq.serialize_element(&self.0)?;
        seq.serialize_element(&self.1.timestamp)?;
        seq.serialize_element(&self.1.supported_schema_ids)?;
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use p2panda_rs::serde::{serialize_from, serialize_value};
    use rstest::rstest;

    use crate::replication::SchemaIdSet;
    use crate::test_utils::helpers::random_schema_id_set;

    use super::{Announcement, AnnouncementMessage};

    #[rstest]
    fn serialize(#[from(random_schema_id_set)] supported_schema_ids: SchemaIdSet) {
        let announcement = Announcement::new(supported_schema_ids.clone());
        assert_eq!(
            serialize_from(AnnouncementMessage::new(announcement.clone())),
            serialize_value(cbor!([0, 1, announcement.timestamp, supported_schema_ids]))
        );
    }
}
