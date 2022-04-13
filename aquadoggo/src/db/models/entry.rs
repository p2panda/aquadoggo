// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use p2panda_rs::storage_provider::errors::EntryStorageError;
use serde::Serialize;
use sqlx::FromRow;

use p2panda_rs::entry::{decode_entry, Entry as P2PandaEntry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{Operation, OperationEncoded};
use p2panda_rs::storage_provider::models::EntryWithOperation;
use p2panda_rs::storage_provider::traits::AsStorageEntry;

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
    type Error = p2panda_rs::storage_provider::errors::ValidationError;

    fn try_from(row: EntryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            author: Author::new(row.author.as_ref())?,
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
    type Error = p2panda_rs::storage_provider::errors::ValidationError;

    fn try_from(row: &EntryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            author: Author::new(row.author.as_ref())?,
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
    type Error = p2panda_rs::storage_provider::errors::ValidationError;

    fn try_into(self) -> Result<EntryWithOperation, Self::Error> {
        EntryWithOperation::new(&self.entry_signed(), &self.operation_encoded().unwrap())
    }
}

impl From<EntryWithOperation> for Entry {
    fn from(entry_with_operation: EntryWithOperation) -> Self {
        let entry = decode_entry(
            entry_with_operation.entry_signed(),
            Some(entry_with_operation.operation_encoded()),
        )
        .unwrap();
        let payload_bytes = entry_with_operation
            .operation_encoded()
            .as_str()
            .to_string();
        let payload_hash = &entry_with_operation.entry_signed().payload_hash();

        Entry {
            author: entry_with_operation.entry_signed().author(),
            entry_bytes: entry_with_operation.entry_signed().as_str().into(),
            entry_hash: entry_with_operation.entry_signed().hash(),
            log_id: *entry.log_id(),
            payload_bytes: Some(payload_bytes),
            payload_hash: payload_hash.clone(),
            seq_num: *entry.seq_num(),
        }
    }
}

impl Entry {
    fn entry_decoded(&self) -> P2PandaEntry {
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

/// Implement `AsStorageEntry` trait for `Entry`
impl AsStorageEntry for Entry {
    type AsStorageEntryError = EntryStorageError;

    fn author(&self) -> Author {
        self.author.clone()
    }

    fn hash(&self) -> Hash {
        self.entry_hash.clone()
    }

    fn entry_bytes(&self) -> Vec<u8> {
        self.entry_signed().to_bytes()
    }

    fn backlink_hash(&self) -> Option<Hash> {
        self.entry_decoded().backlink_hash().cloned()
    }

    fn skiplink_hash(&self) -> Option<Hash> {
        self.entry_decoded().skiplink_hash().cloned()
    }

    fn seq_num(&self) -> SeqNum {
        *self.entry_decoded().seq_num()
    }

    fn log_id(&self) -> LogId {
        *self.entry_decoded().log_id()
    }

    fn operation(&self) -> Operation {
        let operation_encoded = self.operation_encoded().unwrap();
        Operation::from(&operation_encoded)
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

        let schema = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );

        let entries = storage_provider.by_schema(&schema).await.unwrap();
        assert!(entries.len() == 0);
    }
}
