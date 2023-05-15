// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt;

use serde::de::Visitor;
use serde::Deserialize;

use crate::replication::{Mode, SessionId, TargetSet};

pub const SYNC_MESSAGE_TYPE: MessageType = 0;

pub type MessageType = u64;

#[derive(Debug, Clone)]
pub enum Message {
    SyncRequest(Mode, TargetSet),
}

#[derive(Debug, Clone)]
pub struct SyncMessage {
    session_id: SessionId,
    message: Message,
}

impl SyncMessage {
    pub fn new(session_id: SessionId, message: Message) -> Self {
        Self {
            session_id,
            message,
        }
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn message_type(&self) -> MessageType {
        match self.message {
            Message::SyncRequest { .. } => SYNC_MESSAGE_TYPE,
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
                            "too many fields for this replication message",
                        ));
                    }
                };

                Ok(SyncMessage {
                    session_id,
                    message,
                })
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
