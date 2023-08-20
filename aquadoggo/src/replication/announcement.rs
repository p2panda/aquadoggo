// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use p2panda_rs::Validate;
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::replication::{TargetSet, REPLICATION_PROTOCOL_VERSION};

/// u64 timestamp from UNIX epoch until now.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time invalid, operation system time configured before UNIX epoch")
        .as_secs()
}

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
        Self {
            timestamp: now(),
            target_set,
        }
    }
}

pub type ProtocolVersion = u64;

/// Message which can be used to send announcements over the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnouncementMessage(ProtocolVersion, Announcement);

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
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&self.0)?;
        seq.serialize_element(&self.1.timestamp)?;
        seq.serialize_element(&self.1.target_set)?;
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
                let protocol_version: ProtocolVersion = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing protocol version in announce message")
                })?;

                let timestamp: u64 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing timestamp in announce message")
                })?;

                let target_set: TargetSet = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing target set in announce message")
                })?;
                target_set.validate().map_err(|_| {
                    serde::de::Error::custom("invalid target set in announce message")
                })?;

                if let Some(items_left) = seq.size_hint() {
                    if items_left > 0 {
                        return Err(serde::de::Error::custom(
                            "too many fields for announce message",
                        ));
                    }
                };

                Ok(AnnouncementMessage(
                    protocol_version,
                    Announcement {
                        target_set,
                        timestamp,
                    },
                ))
            }
        }

        deserializer.deserialize_seq(MessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use ciborium::value::{Error, Value};
    use p2panda_rs::serde::{deserialize_into, serialize_from, serialize_value};
    use rstest::rstest;

    use crate::replication::TargetSet;
    use crate::test_utils::helpers::random_target_set;

    use super::{Announcement, AnnouncementMessage};

    #[rstest]
    fn serialize(#[from(random_target_set)] target_set: TargetSet) {
        let announcement = Announcement::new(target_set.clone());
        assert_eq!(
            serialize_from(AnnouncementMessage::new(announcement.clone())),
            serialize_value(cbor!([1, announcement.timestamp, target_set]))
        );
    }

    #[rstest]
    fn deserialize(#[from(random_target_set)] target_set: TargetSet) {
        assert_eq!(
            deserialize_into::<AnnouncementMessage>(&serialize_value(cbor!([
                1, 12345678, target_set
            ])))
            .unwrap(),
            AnnouncementMessage::new(Announcement {
                timestamp: 12345678,
                target_set,
            })
        );
    }

    #[rstest]
    #[should_panic(expected = "missing protocol version in announce message")]
    #[case::missing_version(cbor!([]))]
    #[should_panic(expected = "missing timestamp in announce message")]
    #[case::missing_timestamp(cbor!([122]))]
    #[should_panic(expected = "too many fields for announce message")]
    #[case::too_many_fields(cbor!([1, 0, ["schema_field_definition_v1"], "too much"]))]
    fn deserialize_invalid_messages(#[case] cbor: Result<Value, Error>) {
        // Check the cbor is valid
        assert!(cbor.is_ok());

        // We unwrap here to cause a panic and then test for expected error stings
        deserialize_into::<AnnouncementMessage>(&serialize_value(cbor)).unwrap();
    }
}
