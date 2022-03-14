// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use crate::db::models::{Entry, Log};
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{decode_entry, EntrySigned, LogId};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::conversions::{FromStorage, ToStorage};
use p2panda_rs::storage_provider::models::{AsEntry, AsLog};

use crate::errors::Error;

pub struct EntryWithOperation(pub EntrySigned, pub Option<OperationEncoded>);

impl ToStorage<EntryWithOperation> for Entry {
    type ToMemoryStoreError = Error;

    fn to_store_value(input: EntryWithOperation) -> Result<Self, Self::ToMemoryStoreError> {
        Entry::new(&input.0, input.1.as_ref())
    }
}

impl FromStorage<EntryWithOperation> for Entry {
    type FromStorageError = Error;

    fn from_store_value(&self) -> Result<EntryWithOperation, Self::FromStorageError> {
        let operation_encoded = self
            .payload_bytes
            .clone()
            .map(|bytes| OperationEncoded::new(&bytes).unwrap());
        let entry_encoded = EntrySigned::new(&self.entry_bytes).unwrap();
        Ok(EntryWithOperation(entry_encoded, operation_encoded))
    }
}

impl AsEntry<EntryWithOperation> for Entry {
    type Error = Error;

    fn new(entry_encoded: &EntrySigned, operation_encoded: Option<&OperationEncoded>) -> Self {
        let entry = decode_entry(entry_encoded, operation_encoded).unwrap();
        let payload_bytes =
            operation_encoded.map(|operation_encoded| operation_encoded.as_str().to_string());
        let payload_hash = entry_encoded.payload_hash();

        Self {
            author: entry_encoded.author(),
            entry_bytes: entry_encoded.as_str().into(),
            entry_hash: entry_encoded.hash(),
            log_id: *entry.log_id(),
            payload_bytes,
            payload_hash,
            seq_num: *entry.seq_num(),
        }
    }

    fn entry_encoded(&self) -> EntrySigned {
        self.from_store_value().unwrap().0
    }

    fn operation_encoded(&self) -> Option<OperationEncoded> {
        self.from_store_value().unwrap().1
    }
}

pub struct P2PandaLog(Log);

impl ToStorage<P2PandaLog> for Log {
    type ToMemoryStoreError = Error;

    fn to_store_value(input: P2PandaLog) -> Result<Self, Self::ToMemoryStoreError> {
        Ok(Self { ..input.0 })
    }
}

impl FromStorage<P2PandaLog> for Log {
    type FromStorageError = Error;

    fn from_store_value(&self) -> Result<P2PandaLog, Self::FromStorageError> {
        Ok(P2PandaLog(self.clone()))
    }
}

impl AsLog<P2PandaLog> for Log {
    type Error = Error;

    fn new(author: Author, document: DocumentId, schema: SchemaId, log_id: LogId) -> Self {
        let schema_id = match schema {
            SchemaId::Application(pinned_relation) => {
                let mut id_str = "".to_string();
                let mut relation_iter = pinned_relation.into_iter().peekable();
                while let Some(hash) = relation_iter.next() {
                    id_str += hash.as_str();
                    if relation_iter.peek().is_none() {
                        id_str += "_"
                    }
                }
                id_str
            }
            SchemaId::Schema => "schema_v1".to_string(),
            SchemaId::SchemaField => "schema_field_v1".to_string(),
        };

        Self {
            author: author.as_str().to_string(),
            log_id: log_id.as_u64().to_string(),
            document: document.as_str().to_string(),
            schema: schema_id,
        }
    }

    fn author(&self) -> Author {
        Author::new(&self.author).unwrap()
    }
    fn log_id(&self) -> LogId {
        LogId::from_str(&self.log_id).unwrap()
    }
    fn document(&self) -> DocumentId {
        let document_id: DocumentId = self.document.parse().unwrap();
        document_id
    }
    fn schema(&self) -> SchemaId {
        let schema_id: SchemaId = self.document.parse().unwrap();
        schema_id
    }
}
