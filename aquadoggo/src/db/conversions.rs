// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::models::{Entry, Log};
use p2panda_rs::entry::{EntrySigned, LogId};
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::storage_provider::conversions::{FromStorage, ToStorage};
use p2panda_rs::storage_provider::models::{AsEntry, AsLog};

use crate::errors::Error;

pub struct EntryWithOperation(EntrySigned, Option<OperationEncoded>);

impl ToStorage<EntryWithOperation> for Entry {
    type ToMemoryStoreError = Error;

    fn to_store_value(&self, input: EntryWithOperation) -> Result<Self, Self::ToMemoryStoreError> {
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
        self.from_store_value().unwrap().1
    }

    fn entry(&self) -> Result<p2panda_rs::entry::Entry, p2panda_rs::entry::EntrySignedError> {
        p2panda_rs::entry::decode_entry(&self.entry_encoded(), self.operation_encoded().as_ref())
    }
}

pub struct P2PandaLog(Log);

impl ToStorage<P2PandaLog> for Log {
    type ToMemoryStoreError = Error;

    fn to_store_value(&self, input: P2PandaLog) -> Result<Self, Self::ToMemoryStoreError> {
        Ok(Self { ..self.clone() })
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

    fn id(&self) -> &LogId {
        &self.log_id
    }
}
