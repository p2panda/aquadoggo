// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use crate::db::models::{Entry, Log};
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{EntrySigned, LogId};
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

    fn entry_encoded(&self) -> EntrySigned {
        self.from_store_value().unwrap().0
    }

    fn operation_encoded(&self) -> Option<OperationEncoded> {
        self.from_store_value().unwrap().1.clone()
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
