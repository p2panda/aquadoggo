// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use p2panda_rs::Validate;
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::replication::{MessageType, TargetSet, ANNOUNCE_TYPE, REPLICATION_PROTOCOL_VERSION};

/// u64 timestamp from UNIX epoch until now.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time invalid, operation system time configured before UNIX epoch")
        .as_secs()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    /// This contains a list of schema ids this peer allowed to support.
    pub supported_schema_ids: TargetSet,

    /// Timestamp of this announcement. Helps to understand if we can override the previous
    /// announcement with a newer one.
    pub timestamp: u64,
}

impl Announcement {
    pub fn new(supported_schema_ids: TargetSet) -> Self {
        Self {
            timestamp: now(),
            supported_schema_ids,
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
        let mut seq = serializer.serialize_seq(Some(4))?;
        seq.serialize_element(&ANNOUNCE_TYPE)?;
        seq.serialize_element(&self.0)?;
        seq.serialize_element(&self.1.timestamp)?;
        seq.serialize_element(&self.1.supported_schema_ids)?;
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
                let message_type: MessageType = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing message type in announce message")
                })?;

                if message_type != ANNOUNCE_TYPE {
                    return Err(serde::de::Error::custom(
                        "invalid message type for announce message",
                    ));
                }

                let protocol_version: ProtocolVersion = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing protocol version in announce message")
                })?;

                let timestamp: u64 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing timestamp in announce message")
                })?;

                let supported_schema_ids: TargetSet = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing target set in announce message")
                })?;
                supported_schema_ids.validate().map_err(|_| {
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
                        supported_schema_ids,
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

    use crate::replication::SchemaIdSet;
    use crate::test_utils::helpers::random_target_set;

    use super::{Announcement, AnnouncementMessage};

    #[rstest]
    fn serialize(#[from(random_target_set)] supported_schema_ids: SchemaIdSet) {
        let announcement = Announcement::new(supported_schema_ids.clone());
        assert_eq!(
            serialize_from(AnnouncementMessage::new(announcement.clone())),
            serialize_value(cbor!([0, 1, announcement.timestamp, supported_schema_ids]))
        );
    }

    #[rstest]
    fn deserialize(#[from(random_target_set)] supported_schema_ids: SchemaIdSet) {
        assert_eq!(
            deserialize_into::<AnnouncementMessage>(&serialize_value(cbor!([
                0,
                1,
                12345678,
                supported_schema_ids
            ])))
            .unwrap(),
            AnnouncementMessage::new(Announcement {
                timestamp: 12345678,
                supported_schema_ids,
            })
        );
    }

    #[rstest]
    #[should_panic(expected = "missing message type in announce message")]
    #[case::missing_version(cbor!([]))]
    #[should_panic(expected = "missing protocol version in announce message")]
    #[case::missing_version(cbor!([0]))]
    #[should_panic(expected = "missing timestamp in announce message")]
    #[case::missing_timestamp(cbor!([0, 122]))]
    #[should_panic(expected = "too many fields for announce message")]
    #[case::too_many_fields(cbor!([0, 1, 0, ["schema_field_definition_v1"], "too much"]))]
    fn deserialize_invalid_messages(#[case] cbor: Result<Value, Error>) {
        // Check the cbor is valid
        assert!(cbor.is_ok());

        // We unwrap here to cause a panic and then test for expected error stings
        deserialize_into::<AnnouncementMessage>(&serialize_value(cbor)).unwrap();
    }
}
