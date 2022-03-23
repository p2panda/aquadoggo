// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use p2panda_rs::entry::{decode_entry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

use p2panda_rs::storage_provider::models::EntryWithOperation;
use p2panda_rs::storage_provider::traits::AsStorageEntry;
use p2panda_rs::storage_provider::StorageProviderError;
use serde::Serialize;
use sqlx::FromRow;

/// Struct representing the actual SQL row of `Entry`.
///
/// We store the u64 integer values of `log_id` and `seq_num` as strings since not all database
/// backend support large numbers.
#[derive(FromRow, Debug, Serialize, Clone)]
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

impl AsRef<Self> for EntryRow {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Entry of an append-only log based on Bamboo specification. It describes the actual data in the
/// p2p network and is shared between nodes.
///
/// Bamboo entries are the main data type of p2panda. Entries are organized in a distributed,
/// single-writer append-only log structure, created and signed by holders of private keys and
/// stored inside the node database.
///
/// The actual entry data is kept in `entry_bytes` and separated from the `payload_bytes` as the
/// payload can be deleted without affecting the data structures integrity. All other fields like
/// `author`, `payload_hash` etc. can be retrieved from `entry_bytes` but are separately stored in
/// the database for faster querying.
#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    /// Public key of the author.
    pub author: Author,

    /// Actual Bamboo entry data.
    pub entry_bytes: String,

    /// Hash of Bamboo entry data.
    pub entry_hash: Hash,

    /// Used log for this entry.
    pub log_id: LogId,

    /// Payload of entry, can be deleted.
    pub payload_bytes: Option<String>,

    /// Hash of payload data.
    pub payload_hash: Hash,

    /// Sequence number of this entry.
    pub seq_num: SeqNum,
}

/// Convert SQL row representation `EntryRow` to typed `Entry` one.
impl TryFrom<EntryRow> for Entry {
    type Error = crate::errors::ValidationErrors;

    fn try_from(row: EntryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            author: Author::try_from(row.author.as_ref())?,
            entry_bytes: row.entry_bytes.clone(),
            entry_hash: row.entry_hash.parse()?,
            log_id: row.log_id.parse()?,
            payload_bytes: row.payload_bytes.clone(),
            payload_hash: row.payload_hash.parse()?,
            seq_num: row.seq_num.parse()?,
        })
    }
}

/// Convert SQL row representation `EntryRow` to typed `Entry` one.
impl TryFrom<&EntryRow> for Entry {
    type Error = crate::errors::ValidationErrors;

    fn try_from(row: &EntryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            author: Author::try_from(row.author.as_ref())?,
            entry_bytes: row.entry_bytes.clone(),
            entry_hash: row.entry_hash.parse()?,
            log_id: row.log_id.parse()?,
            payload_bytes: row.payload_bytes.clone(),
            payload_hash: row.payload_hash.parse()?,
            seq_num: row.seq_num.parse()?,
        })
    }
}

impl TryInto<EntryWithOperation> for Entry {
    type Error = StorageProviderError;

    fn try_into(self) -> Result<EntryWithOperation, Self::Error> {
        EntryWithOperation::new(&self.entry_encoded(), &self.operation_encoded().unwrap())
    }
}

impl From<EntryWithOperation> for Entry {
    fn from(entry_with_operation: EntryWithOperation) -> Self {
        let entry = decode_entry(
            entry_with_operation.entry_encoded(),
            Some(entry_with_operation.operation_encoded()),
        )
        .unwrap();
        let payload_bytes = entry_with_operation
            .operation_encoded()
            .as_str()
            .to_string();
        let payload_hash = &entry_with_operation.entry_encoded().payload_hash();

        Entry {
            author: entry_with_operation.entry_encoded().author(),
            entry_bytes: entry_with_operation.entry_encoded().as_str().into(),
            entry_hash: entry_with_operation.entry_encoded().hash(),
            log_id: *entry.log_id(),
            payload_bytes: Some(payload_bytes),
            payload_hash: payload_hash.clone(),
            seq_num: *entry.seq_num(),
        }
    }
}

impl AsStorageEntry for Entry {
    type AsStorageEntryError = StorageProviderError;

    fn entry_encoded(&self) -> EntrySigned {
        EntrySigned::new(&self.entry_bytes).unwrap()
    }

    fn operation_encoded(&self) -> Option<OperationEncoded> {
        Some(OperationEncoded::new(&self.payload_bytes.clone().unwrap()).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::LogId;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::Author;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::EntryStore;

    use crate::db::sql_storage::SqlStorage;
    use crate::test_helpers::initialize_db;

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[tokio::test]
    async fn latest_entry() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();
        let log_id = LogId::new(1);

        let latest_entry = storage_provider
            .latest_entry(&author, &log_id)
            .await
            .unwrap();
        assert!(latest_entry.is_none());
    }

    #[tokio::test]
    async fn entries_by_schema() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let schema = SchemaId::new(Hash::new_from_bytes(vec![1, 2, 3]).unwrap().as_str()).unwrap();

        let entries = storage_provider.by_schema(&schema).await.unwrap();
        assert!(entries.len() == 0);
    }
}
