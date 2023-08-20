// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt;

use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::Human;
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::replication::{Mode, SessionId, TargetSet};

pub const ANNOUNCE_TYPE: MessageType = 0;
pub const SYNC_REQUEST_TYPE: MessageType = 1;
pub const SYNC_DONE_TYPE: MessageType = 2;
pub const ENTRY_TYPE: MessageType = 3;
pub const HAVE_TYPE: MessageType = 10;

pub type MessageType = u64;

pub type Timestamp = u64;

pub type LiveMode = bool;

pub type LogHeights = (PublicKey, Vec<(LogId, SeqNum)>);

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Message {
    Announce(Timestamp, TargetSet),
    SyncRequest(Mode, TargetSet),
    Have(Vec<LogHeights>),
    Entry(EncodedEntry, Option<EncodedOperation>),
    SyncDone(LiveMode),
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Announce(_, _) => ANNOUNCE_TYPE,
            Message::SyncRequest(_, _) => SYNC_REQUEST_TYPE,
            Message::Have(_) => HAVE_TYPE,
            Message::Entry(_, _) => ENTRY_TYPE,
            Message::SyncDone(_) => SYNC_DONE_TYPE,
        }
    }
}

impl Human for Message {
    fn display(&self) -> String {
        match &self {
            Message::Have(log_heights) => {
                let log_heights: Vec<(String, &Vec<(LogId, SeqNum)>)> = log_heights
                    .iter()
                    .map(|(public_key, log_heights)| (public_key.to_string(), log_heights))
                    .collect();
                format!("Have({log_heights:?})")
            }
            message => format!("{message:?}"),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SyncMessage(SessionId, Message);

impl SyncMessage {
    pub fn new(session_id: SessionId, message: Message) -> Self {
        Self(session_id, message)
    }

    pub fn message_type(&self) -> MessageType {
        self.1.message_type()
    }

    pub fn session_id(&self) -> SessionId {
        self.0
    }

    pub fn message(&self) -> &Message {
        &self.1
    }
}

impl Human for SyncMessage {
    fn display(&self) -> String {
        format!("SyncMessage({:?}, {})", self.0, self.1.display())
    }
}

impl Serialize for SyncMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Always encode message type and session id first
        let serialize_header = |mut seq: <S as serde::Serializer>::SerializeSeq| -> Result<<S as serde::Serializer>::SerializeSeq, S::Error> {
            seq.serialize_element(&self.message_type())?;
            seq.serialize_element(&self.session_id())?;
            Ok(seq)
        };

        match self.message() {
            Message::Announce(timestamp, target_set) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(4))?)?;
                seq.serialize_element(timestamp)?;
                seq.serialize_element(target_set)?;
                seq.end()
            }
            Message::SyncRequest(mode, target_set) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(4))?)?;
                seq.serialize_element(mode)?;
                seq.serialize_element(target_set)?;
                seq.end()
            }
            Message::Have(log_heights) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(3))?)?;
                seq.serialize_element(log_heights)?;
                seq.end()
            }
            Message::Entry(entry_bytes, operation_bytes) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(4))?)?;
                seq.serialize_element(entry_bytes)?;
                seq.serialize_element(operation_bytes)?;
                seq.end()
            }
            Message::SyncDone(live_mode) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(3))?)?;
                seq.serialize_element(live_mode)?;
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SyncMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SyncMessageVisitor;

        impl<'de> Visitor<'de> for SyncMessageVisitor {
            type Value = SyncMessage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("p2panda replication message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let message_type: MessageType = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing message type in replication message")
                })?;

                let session_id: SessionId = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing session id in replication message")
                })?;

                let message = if message_type == ANNOUNCE_TYPE {
                    let timestamp: Timestamp = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing timestamp in announce message")
                    })?;

                    let target_set: TargetSet = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing target set in announce message")
                    })?;

                    Ok(Message::Announce(timestamp, target_set))
                } else if message_type == SYNC_REQUEST_TYPE {
                    let mode: Mode = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing mode in sync request message")
                    })?;

                    let target_set: TargetSet = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing target set in sync request message")
                    })?;

                    Ok(Message::SyncRequest(mode, target_set))
                } else if message_type == ENTRY_TYPE {
                    let entry_bytes: EncodedEntry = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing entry bytes in entry message")
                    })?;

                    let operation_bytes: Option<EncodedOperation> = seq.next_element()?;

                    Ok(Message::Entry(entry_bytes, operation_bytes))
                } else if message_type == SYNC_DONE_TYPE {
                    let live_mode: bool = seq.next_element()?.ok_or_else(|| {
                        serde::de::Error::custom("missing live mode flag in sync done message")
                    })?;

                    Ok(Message::SyncDone(live_mode))
                } else if message_type == HAVE_TYPE {
                    let log_heights: Vec<(PublicKey, Vec<(LogId, SeqNum)>)> =
                        seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing log heights in have message")
                        })?;

                    Ok(Message::Have(log_heights))
                } else {
                    Err(serde::de::Error::custom(format!(
                        "unknown message type {} in replication message",
                        message_type
                    )))
                }?;

                if let Some(items_left) = seq.size_hint() {
                    if items_left > 0 {
                        return Err(serde::de::Error::custom(
                            "too many fields for replication message",
                        ));
                    }
                };

                Ok(SyncMessage::new(session_id, message))
            }
        }

        deserializer.deserialize_seq(SyncMessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use ciborium::value::{Error, Value};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::serde::{deserialize_into, serialize_from, serialize_value};
    use p2panda_rs::test_utils::fixtures::public_key;
    use rstest::rstest;

    use crate::replication::{Mode, TargetSet};
    use crate::test_utils::helpers::random_target_set;

    use super::{Message, SyncMessage};

    #[rstest]
    fn serialize(#[from(random_target_set)] target_set: TargetSet, public_key: PublicKey) {
        assert_eq!(
            serialize_from(SyncMessage::new(
                51,
                Message::SyncRequest(Mode::SetReconciliation, target_set.clone())
            )),
            serialize_value(cbor!([1, 51, 1, target_set]))
        );

        assert_eq!(
            serialize_from(SyncMessage::new(
                51,
                Message::Have(vec![(
                    public_key,
                    vec![(LogId::default(), SeqNum::default())]
                )])
            )),
            serialize_value(cbor!([
                10,
                51,
                vec![(
                    // Convert explicitly to bytes as `cbor!` macro doesn't understand somehow that
                    // `PublicKey` serializes to a byte array
                    serde_bytes::Bytes::new(&public_key.to_bytes()),
                    vec![(LogId::default(), SeqNum::default())]
                )]
            ]))
        );
    }

    #[rstest]
    fn deserialize(#[from(random_target_set)] target_set: TargetSet, public_key: PublicKey) {
        assert_eq!(
            deserialize_into::<SyncMessage>(&serialize_value(cbor!([1, 12, 0, target_set])))
                .unwrap(),
            SyncMessage::new(
                12,
                Message::SyncRequest(Mode::LogHeight, target_set.clone())
            )
        );

        let log_heights: Vec<(PublicKey, Vec<(LogId, SeqNum)>)> = vec![];
        assert_eq!(
            deserialize_into::<SyncMessage>(&serialize_value(cbor!([10, 12, log_heights])))
                .unwrap(),
            SyncMessage::new(12, Message::Have(vec![]))
        );

        assert_eq!(
            deserialize_into::<SyncMessage>(&serialize_value(cbor!([
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
            SyncMessage::new(
                12,
                Message::Have(vec![(
                    public_key,
                    vec![(LogId::default(), SeqNum::default())]
                )])
            )
        );
    }

    #[rstest]
    #[should_panic(expected = "missing message type in replication message")]
    #[case::no_fields(cbor!([]))]
    #[should_panic(expected = "unknown message type 122 in replication message")]
    #[case::unknown_message_type(cbor!([122, 0]))]
    #[should_panic(expected = "missing session id in replication message")]
    #[case::only_message_type(cbor!([0]))]
    #[should_panic(expected = "too many fields for replication message")]
    #[case::too_many_fields(cbor!([0, 0, 0, ["schema_field_definition_v1"], "too much"]))]
    fn deserialize_invalid_messages(#[case] cbor: Result<Value, Error>) {
        // Check the cbor is valid
        assert!(cbor.is_ok());

        // Deserialize into sync message, we unwrap here to cause a panic and then test for
        // expected error stings
        deserialize_into::<SyncMessage>(&serialize_value(cbor)).unwrap();
    }
}
