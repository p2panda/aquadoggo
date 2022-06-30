// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::{anyhow, Error, Result};
use async_graphql::SimpleObject;
use p2panda_rs::storage_provider::traits::AsStorageEntry;
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

impl TryFrom<EncodedEntryAndOperation> for StorageEntry {
    type Error = Error;

    fn try_from(encoded: EncodedEntryAndOperation) -> Result<StorageEntry> {
        let operation = encoded
            .operation
            .ok_or(anyhow!("Storage entry requires operation to be given"))?;

        Ok(StorageEntry::new(&encoded.entry.into(), &operation.into())?)
    }
}
