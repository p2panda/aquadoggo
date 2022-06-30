// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};

use crate::db::stores::StorageEntry;
use crate::graphql::scalars;

/// Encoded and signed entry with its regarding encoded operation payload.
#[derive(SimpleObject, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct EncodedEntryAndOperation {
    /// Signed and encoded bamboo entry.
    pub entry: scalars::EncodedEntry,

    /// p2panda operation, CBOR bytes encoded as hexadecimal string.
    pub operation: Option<scalars::EncodedOperation>,
}

impl From<StorageEntry> for EncodedEntryAndOperation {
    fn from(entry_row: StorageEntry) -> Self {
        let entry = entry_row.entry_signed().to_owned().into();
        let operation = entry_row.operation_encoded().map(|op| op.to_owned().into());
        Self { entry, operation }
    }
}
