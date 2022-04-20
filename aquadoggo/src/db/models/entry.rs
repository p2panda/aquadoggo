// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::{query, query_as, query_scalar};

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{decode_entry, Entry as P2PandaEntry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{Operation, OperationEncoded};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::{EntryStorageError, ValidationError};
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, StorageProvider};
use p2panda_rs::Validate;

use crate::db::models::Log;
use crate::db::store::SqlStorage;
use crate::errors::StorageProviderResult;
use crate::rpc::{EntryArgsRequest, EntryArgsResponse, PublishEntryRequest, PublishEntryResponse};

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

impl EntryRow {
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
impl AsStorageEntry for EntryRow {
    type AsStorageEntryError = EntryStorageError;

    fn new(
        entry_signed: &EntrySigned,
        operation_encoded: &OperationEncoded,
    ) -> Result<Self, Self::AsStorageEntryError> {
        let entry = decode_entry(entry_signed, Some(operation_encoded))?;

        Ok(Self {
            author: entry_signed.author().as_str().into(),
            entry_bytes: entry_signed.as_str().into(),
            entry_hash: entry_signed.hash().as_str().into(),
            log_id: entry.log_id().as_u64().to_string(),
            payload_bytes: Some(operation_encoded.as_str().to_string()),
            payload_hash: entry_signed.payload_hash().as_str().into(),
            seq_num: entry.seq_num().as_u64().to_string(),
        })
    }

    fn author(&self) -> Author {
        Author::new(self.author.as_ref()).unwrap()
    }

    fn hash(&self) -> Hash {
        self.entry_signed().hash()
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

/// Trait which handles all storage actions relating to `Entries`.
#[async_trait]
impl EntryStore<EntryRow> for SqlStorage {
    /// Insert an entry into storage.
    async fn insert_entry(&self, entry: EntryRow) -> Result<bool, EntryStorageError> {
        println!("{:?}", entry);
        let rows_affected = query(
            "
            INSERT INTO
                entries (
                    author,
                    entry_bytes,
                    entry_hash,
                    log_id,
                    payload_bytes,
                    payload_hash,
                    seq_num
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(entry.author().as_str())
        .bind(entry.entry_signed().as_str())
        .bind(entry.hash().as_str())
        .bind(entry.log_id().as_u64().to_string())
        .bind(entry.operation_encoded().unwrap().as_str())
        .bind(entry.operation_encoded().unwrap().hash().as_str())
        .bind(entry.seq_num().as_u64().to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Returns entry at sequence position within an author's log.
    async fn entry_at_seq_num(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<EntryRow>, EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
                AND seq_num = $3
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row)
    }

    /// Returns the latest Bamboo entry of an author's log.
    async fn latest_entry(
        &self,
        author: &Author,
        log_id: &LogId,
    ) -> Result<Option<EntryRow>, EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
            ORDER BY
                seq_num DESC
            LIMIT
                1
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row)
    }

    /// Return vector of all entries of a given schema
    async fn by_schema(&self, schema: &SchemaId) -> Result<Vec<EntryRow>, EntryStorageError> {
        let entries = query_as::<_, EntryRow>(
            "
            SELECT
                entries.author,
                entries.entry_bytes,
                entries.entry_hash,
                entries.log_id,
                entries.payload_bytes,
                entries.payload_hash,
                entries.seq_num
            FROM
                entries
            INNER JOIN logs
                ON (entries.log_id = logs.log_id
                    AND entries.author = logs.author)
            WHERE
                logs.schema = $1
            ",
        )
        .bind(schema.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entries)
    }
}

/// All other methods needed to be implemented by a p2panda `StorageProvider`
#[async_trait]
impl StorageProvider<EntryRow, Log> for SqlStorage {
    type EntryArgsResponse = EntryArgsResponse;
    type EntryArgsRequest = EntryArgsRequest;
    type PublishEntryResponse = PublishEntryResponse;
    type PublishEntryRequest = PublishEntryRequest;

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document id by looking up the log
    /// that the entry was stored in.
    async fn get_document_by_entry(
        &self,
        entry_hash: &Hash,
    ) -> StorageProviderResult<Option<DocumentId>> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                logs.document
            FROM
                logs
            INNER JOIN entries
                ON (logs.log_id = entries.log_id
                    AND logs.author = entries.author)
            WHERE
                entries.entry_hash = $1
            ",
        )
        .bind(entry_hash.as_str())
        .fetch_optional(&self.pool)
        .await?;

        // Unwrap here since we already validated the hash
        let hash = result.map(|str| {
            Hash::new(&str)
                .expect("Corrupt hash found in database")
                .into()
        });

        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::LogId;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::Author;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::EntryStore;

    use crate::db::store::SqlStorage;
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
