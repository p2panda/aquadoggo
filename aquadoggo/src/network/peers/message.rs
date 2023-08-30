// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::{EncodedEntry, LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::Validate;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};

use crate::replication::{
    Announcement, AnnouncementMessage, Message, Mode, SchemaIdSet, SessionId, SyncMessage,
    ANNOUNCE_TYPE, ENTRY_TYPE, HAVE_TYPE, SYNC_DONE_TYPE, SYNC_REQUEST_TYPE,
};

/// p2panda protocol messages which can be sent over the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PeerMessage {
    /// Announcement of peers about the schema ids they are interest in.
    Announce(AnnouncementMessage),

    /// Replication status and data exchange.
    SyncMessage(SyncMessage),
}

impl<'de> Deserialize<'de> for PeerMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = PeerMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("p2panda message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let message_type: u64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("invalid message type"))?;

                let message = match message_type {
                    ANNOUNCE_TYPE => {
                        let protocol_version: u64 = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing protocol version in announce message")
                        })?;

                        let timestamp: u64 = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing timestamp in announce message")
                        })?;

                        let supported_schema_ids: SchemaIdSet =
                            seq.next_element()?.ok_or_else(|| {
                                serde::de::Error::custom("missing target set in announce message")
                            })?;
                        supported_schema_ids.validate().map_err(|_| {
                            serde::de::Error::custom("invalid target set in announce message")
                        })?;

                        PeerMessage::Announce(AnnouncementMessage(
                            protocol_version,
                            Announcement {
                                supported_schema_ids,
                                timestamp,
                            },
                        ))
                    }
                    SYNC_REQUEST_TYPE => {
                        let session_id: SessionId = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing session id in replication message")
                        })?;

                        let mode: Mode = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing mode in sync request message")
                        })?;

                        let target_set: SchemaIdSet = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing target set in sync request message")
                        })?;

                        target_set.validate().map_err(|_| {
                            serde::de::Error::custom("invalid target set in sync request message")
                        })?;

                        if target_set.is_empty() {
                            return Err(serde::de::Error::custom(
                                "empty target set in sync request message",
                            ));
                        }

                        PeerMessage::SyncMessage(SyncMessage::new(
                            session_id,
                            Message::SyncRequest(mode, target_set),
                        ))
                    }
                    ENTRY_TYPE => {
                        let session_id: SessionId = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing session id in replication message")
                        })?;

                        let entry_bytes: EncodedEntry = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing entry bytes in entry message")
                        })?;

                        let operation_bytes: Option<EncodedOperation> = seq.next_element()?;

                        PeerMessage::SyncMessage(SyncMessage::new(
                            session_id,
                            Message::Entry(entry_bytes, operation_bytes),
                        ))
                    }
                    SYNC_DONE_TYPE => {
                        let session_id: SessionId = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing session id in replication message")
                        })?;

                        let live_mode: bool = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing live mode flag in sync done message")
                        })?;

                        PeerMessage::SyncMessage(SyncMessage::new(
                            session_id,
                            Message::SyncDone(live_mode),
                        ))
                    }
                    HAVE_TYPE => {
                        let session_id: SessionId = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing session id in replication message")
                        })?;

                        let log_heights: Vec<(PublicKey, Vec<(LogId, SeqNum)>)> =
                            seq.next_element()?.ok_or_else(|| {
                                serde::de::Error::custom("missing log heights in have message")
                            })?;

                        PeerMessage::SyncMessage(SyncMessage::new(
                            session_id,
                            Message::Have(log_heights),
                        ))
                    }
                    _ => return Err(serde::de::Error::custom("unknown message type")),
                };

                if let Some(items_left) = seq.size_hint() {
                    if items_left > 0 {
                        return Err(serde::de::Error::custom(
                            "too many fields for p2panda message",
                        ));
                    }
                };

                Ok(message)
            }
        }

        deserializer.deserialize_seq(MessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use ciborium::value::{Error, Value};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::serde::{deserialize_into, serialize_value};
    use p2panda_rs::test_utils::fixtures::public_key;
    use rstest::rstest;

    use crate::replication::{
        Announcement, AnnouncementMessage, Message, Mode, SchemaIdSet, SyncMessage,
    };
    use crate::test_utils::helpers::random_schema_id_set;

    use super::PeerMessage;

    #[rstest]
    fn deserialize(
        #[from(random_schema_id_set)] supported_schema_ids: SchemaIdSet,
        #[from(random_schema_id_set)] target_set: SchemaIdSet,
        public_key: PublicKey,
    ) {
        assert_eq!(
            deserialize_into::<PeerMessage>(&serialize_value(cbor!([
                0,
                1,
                12345678,
                supported_schema_ids
            ])))
            .unwrap(),
            PeerMessage::Announce(AnnouncementMessage::new(Announcement {
                timestamp: 12345678,
                supported_schema_ids,
            }))
        );

        assert_eq!(
            deserialize_into::<PeerMessage>(&serialize_value(cbor!([1, 12, 0, target_set])))
                .unwrap(),
            PeerMessage::SyncMessage(SyncMessage::new(
                12,
                Message::SyncRequest(Mode::LogHeight, target_set.clone())
            ))
        );

        let log_heights: Vec<(PublicKey, Vec<(LogId, SeqNum)>)> = vec![];
        assert_eq!(
            deserialize_into::<PeerMessage>(&serialize_value(cbor!([10, 12, log_heights])))
                .unwrap(),
            PeerMessage::SyncMessage(SyncMessage::new(12, Message::Have(vec![])))
        );

        assert_eq!(
            deserialize_into::<PeerMessage>(&serialize_value(cbor!([
                10,
                12,
                vec![(
                    // Convert explicitly to bytes as `cbor!` macro doesn't understand somehow that
                    // `PublicKey` serializes to a byte array
                    serde_bytes::Bytes::new(&public_key.to_bytes()),
                    vec![(LogId::default(), SeqNum::default())]
                )]
            ])))
            .unwrap(),
            PeerMessage::SyncMessage(SyncMessage::new(
                12,
                Message::Have(vec![(
                    public_key,
                    vec![(LogId::default(), SeqNum::default())]
                )])
            ))
        );
    }

    #[rstest]
    #[should_panic(expected = "invalid message type")]
    #[case::invalid_message_type(cbor!([]))]
    #[should_panic(expected = "missing protocol version in announce message")]
    #[case::announce_missing_version(cbor!([0]))]
    #[should_panic(expected = "missing timestamp in announce message")]
    #[case::announce_missing_timestamp(cbor!([0, 122]))]
    #[should_panic(expected = "too many fields for p2panda message")]
    #[case::announce_too_many_fields(cbor!([0, 1, 0, ["schema_field_definition_v1"], "too much"]))]
    #[should_panic(expected = "missing session id in replication message")]
    #[case::sync_only_message_type(cbor!([1]))]
    #[should_panic(expected = "empty target set in sync request")]
    #[case::sync_only_message_type(cbor!([1, 0, 0, []]))]
    #[should_panic(expected = "too many fields for p2panda message")]
    #[case::sync_too_many_fields(cbor!([1, 0, 0, ["schema_field_definition_v1"], "too much"]))]
    fn deserialize_invalid_messages(#[case] cbor: Result<Value, Error>) {
        // Check the cbor is valid
        assert!(cbor.is_ok());

        // We unwrap here to cause a panic and then test for expected error stings
        deserialize_into::<PeerMessage>(&serialize_value(cbor)).unwrap();
    }
}
