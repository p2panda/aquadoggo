// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::Error;
use async_graphql::*;
use p2panda_rs::storage_provider::traits::AsStorageEntry;

use crate::db::stores::StorageEntry;

use super::payload::Payload;
use super::Entry;

/// An entry with an optional payload
#[derive(SimpleObject, Debug)]
pub struct EntryAndPayload {
    /// Get the entry
    pub entry: Entry,
    /// Get the payload
    pub payload: Option<Payload>,
}

impl From<StorageEntry> for EntryAndPayload {
    fn from(entry_row: StorageEntry) -> Self {
        let entry = Entry(entry_row.entry_signed().to_owned());
        let payload = entry_row
            .operation_encoded()
            .map(|encoded| Payload(encoded.to_owned()));
        Self { entry, payload }
    }
}

impl TryFrom<EntryAndPayload> for StorageEntry {
    type Error = Error;

    fn try_from(entry_and_payload: EntryAndPayload) -> Result<Self, Self::Error> {
        let entry_signed = entry_and_payload.entry.0;

        //TODO: need to find out why StorageEntry enforces having the operation
        let operation_encoded = entry_and_payload.payload.unwrap().0;

        let result = StorageEntry::new(&entry_signed, &operation_encoded)?;

        Ok(result)
    }
}
