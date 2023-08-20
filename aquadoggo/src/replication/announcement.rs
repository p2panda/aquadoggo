// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use p2panda_rs::Validate;
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::replication::{TargetSet, REPLICATION_PROTOCOL_VERSION};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    /// This contains a list of schema ids this peer is interested in.
    pub target_set: TargetSet,

    /// Timestamp of this announcement. Helps to understand if we can override the previous
    /// announcement with a newer one.
    pub timestamp: u64,
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

/// Message which can be used to send announcements over the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnouncementMessage(pub Announcement);

impl Serialize for AnnouncementMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&REPLICATION_PROTOCOL_VERSION)?;
        seq.serialize_element(&self.0.timestamp)?;
        seq.serialize_element(&self.0.target_set)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for AnnouncementMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = AnnouncementMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("p2panda announce message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let version: u64 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing version in announce message")
                })?;
                if version != REPLICATION_PROTOCOL_VERSION {
                    return Err(serde::de::Error::custom("invalid announce message version"));
                }

                let timestamp: u64 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing timestamp in announce message")
                })?;

                let target_set: TargetSet = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing target set in announce message")
                })?;
                target_set.validate().map_err(|_| {
                    serde::de::Error::custom("invalid target set in announce message")
                })?;

                Ok(AnnouncementMessage(Announcement {
                    target_set,
                    timestamp,
                }))
            }
        }

        deserializer.deserialize_seq(MessageVisitor)
    }
}
