// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt;

use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::replication::{Mode, SessionId, TargetSet};

pub const SYNC_MESSAGE_TYPE: MessageType = 0;

pub type MessageType = u64;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Message {
    SyncRequest(Mode, TargetSet),
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::SyncRequest { .. } => SYNC_MESSAGE_TYPE,
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
            Message::SyncRequest(mode, target_set) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(4))?)?;
                seq.serialize_element(mode)?;
                seq.serialize_element(target_set)?;
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

                let message = match message_type {
                    SYNC_MESSAGE_TYPE => {
                        let mode: Mode = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing mode in sync request message")
                        })?;

                        let target_set: TargetSet = seq.next_element()?.ok_or_else(|| {
                            serde::de::Error::custom("missing target set in sync request message")
                        })?;

                        Ok(Message::SyncRequest(mode, target_set))
                    }
                    unknown_type => Err(serde::de::Error::custom(format!(
                        "unknown message type {} in replication message",
                        unknown_type
                    ))),
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

#[derive(Clone, Debug)]
pub enum StrategyMessage {
    Have,
    Entry,
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use ciborium::value::{Error, Value};
    use p2panda_rs::serde::{deserialize_into, serialize_from, serialize_value};
    use rstest::rstest;

    use crate::replication::{Mode, TargetSet};
    use crate::test_utils::helpers::random_target_set;

    use super::{Message, SyncMessage};

    #[rstest]
    fn serialize(#[from(random_target_set)] target_set: TargetSet) {
        assert_eq!(
            serialize_from(SyncMessage::new(
                51,
                Message::SyncRequest(Mode::SetReconciliation, target_set.clone())
            )),
            serialize_value(cbor!([0, 51, 1, target_set]))
        );
    }

    #[rstest]
    fn deserialize(#[from(random_target_set)] target_set: TargetSet) {
        assert_eq!(
            deserialize_into::<SyncMessage>(&serialize_value(cbor!([0, 12, 0, target_set])))
                .unwrap(),
            SyncMessage::new(12, Message::SyncRequest(Mode::Naive, target_set.clone()))
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
