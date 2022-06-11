// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;

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
