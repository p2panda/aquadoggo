// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::{decode_entry, Entry, EntrySigned};
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::storage_provider::errors::ValidationError;
use p2panda_rs::Validate;
use serde::Serialize;
use sqlx::FromRow;

/// Struct representing the actual SQL row of `Entry`.
///
/// We store the u64 integer values of `log_id` and `seq_num` as strings since not all database
/// backend support large numbers.
#[derive(FromRow, Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EntryRow {
    /// Public key of the author.
    pub author: String,

    /// Actual Bamboo entry data.
    pub entry_bytes: String,

    /// Hash of Bamboo entry data.
    pub entry_hash: String,

    /// Used log for this entry.
    pub log_id: String,

    /// Payload of entry, can be deleted.
    pub payload_bytes: Option<String>,

    /// Hash of payload data.
    pub payload_hash: String,

    /// Sequence number of this entry.
    pub seq_num: String,
}

impl EntryRow {
    pub fn entry_decoded(&self) -> Entry {
        // Unwrapping as validation occurs in `EntryWithOperation`.
        decode_entry(&self.entry_signed(), self.operation_encoded().as_ref()).unwrap()
    }

    pub fn entry_signed(&self) -> EntrySigned {
        EntrySigned::new(&self.entry_bytes).unwrap()
    }

    pub fn operation_encoded(&self) -> Option<OperationEncoded> {
        Some(OperationEncoded::new(&self.payload_bytes.clone().unwrap()).unwrap())
    }
}

impl Validate for EntryRow {
    type Error = ValidationError;

    fn validate(&self) -> Result<(), Self::Error> {
        self.entry_signed().validate()?;
        if let Some(operation) = self.operation_encoded() {
            operation.validate()?;
        }
        decode_entry(&self.entry_signed(), self.operation_encoded().as_ref())?;
        Ok(())
    }
}

impl AsRef<Self> for EntryRow {
    fn as_ref(&self) -> &Self {
        self
    }
}
