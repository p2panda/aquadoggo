// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::Human;
use serde::ser::SerializeSeq;
use serde::Serialize;
use unionize::protocol::Message as UnionizeMessage;

use crate::replication::strategies::set_reconciliation::{Item, Monoid};
use crate::replication::{
    MessageType, Mode, SchemaIdSet, SessionId, ENTRY_TYPE, HAVE_TYPE, SET_RECONCILIATION_TYPE,
    SYNC_DONE_TYPE, SYNC_REQUEST_TYPE,
};

pub type LiveMode = bool;

pub type LogHeights = (PublicKey, Vec<(LogId, SeqNum)>);

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Message {
    SyncRequest(Mode, SchemaIdSet),
    Entry(EncodedEntry, Option<EncodedOperation>),
    SyncDone(LiveMode),
    SetReconciliation(UnionizeMessage<Monoid, (Item, bool)>),
    Have(Vec<LogHeights>),
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::SyncRequest(_, _) => SYNC_REQUEST_TYPE,
            Message::Entry(_, _) => ENTRY_TYPE,
            Message::SyncDone(_) => SYNC_DONE_TYPE,
            Message::SetReconciliation(_) => SET_RECONCILIATION_TYPE,
            Message::Have(_) => HAVE_TYPE,
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
            Message::SyncRequest(mode, target_set) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(4))?)?;
                seq.serialize_element(mode)?;
                seq.serialize_element(target_set)?;
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
            Message::SetReconciliation(unionize_message) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(3))?)?;
                seq.serialize_element(unionize_message)?;
                seq.end()
            }
            Message::Have(log_heights) => {
                let mut seq = serialize_header(serializer.serialize_seq(Some(3))?)?;
                seq.serialize_element(log_heights)?;
                seq.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::serde::{serialize_from, serialize_value};
    use p2panda_rs::test_utils::fixtures::public_key;
    use rstest::rstest;

    use crate::replication::{Mode, SchemaIdSet};
    use crate::test_utils::helpers::random_schema_id_set;

    use super::{Message, SyncMessage};

    #[rstest]
    fn serialize(#[from(random_schema_id_set)] target_set: SchemaIdSet, public_key: PublicKey) {
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
}
