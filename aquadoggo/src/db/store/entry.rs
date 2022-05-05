// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::{query, query_as};

use p2panda_rs::entry::{decode_entry, Entry as P2PandaEntry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{Operation, OperationEncoded};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::{EntryStorageError, ValidationError};
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
use p2panda_rs::Validate;

use crate::db::sql_store::SqlStorage;

/// Struct representing the actual SQL row of `Entry`.
///
/// We store the u64 integer values of `log_id` and `seq_num` as strings since not all database
/// backend support large numbers.
#[derive(FromRow, Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DoggoEntry {
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

impl AsRef<Self> for DoggoEntry {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl DoggoEntry {
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
impl AsStorageEntry for DoggoEntry {
    type AsStorageEntryError = EntryStorageError;

    fn new(
        entry_signed: &EntrySigned,
        operation_encoded: &OperationEncoded,
    ) -> Result<Self, Self::AsStorageEntryError> {
        let entry = decode_entry(entry_signed, Some(operation_encoded))
            .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

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

impl Validate for DoggoEntry {
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
impl EntryStore<DoggoEntry> for SqlStorage {
    /// Insert an entry into storage.
    async fn insert_entry(&self, entry: DoggoEntry) -> Result<bool, EntryStorageError> {
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

    async fn get_entry_by_hash(
        &self,
        hash: &Hash,
    ) -> Result<Option<DoggoEntry>, EntryStorageError> {
        let entry_row = query_as::<_, DoggoEntry>(
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
                entry_hash = $1
            ",
        )
        .bind(hash.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row)
    }

    /// Returns entry at sequence position within an author's log.
    async fn entry_at_seq_num(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<DoggoEntry>, EntryStorageError> {
        let entry_row = query_as::<_, DoggoEntry>(
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
    ) -> Result<Option<DoggoEntry>, EntryStorageError> {
        let entry_row = query_as::<_, DoggoEntry>(
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
                CAST(seq_num AS INTEGER) DESC
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
    async fn by_schema(&self, schema: &SchemaId) -> Result<Vec<DoggoEntry>, EntryStorageError> {
        let entries = query_as::<_, DoggoEntry>(
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

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::str::FromStr;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::{sign_and_encode, Entry};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, StorageProvider};
    use p2panda_rs::test_utils::constants::{DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};

    use crate::db::store::entry::DoggoEntry;
    use crate::db::store::test_utils::test_db;
    use crate::rpc::EntryArgsRequest;

    #[tokio::test]
    async fn insert_entry() {
        let storage_provider = test_db().await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let log_id = LogId::new(1);
        let schema = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        // Derive the document_id by fetching the first entry
        let document_id: DocumentId = storage_provider
            .entry_at_seq_num(&author, &log_id, &SeqNum::new(1).unwrap())
            .await
            .unwrap()
            .unwrap()
            .hash()
            .into();

        let next_entry_args = storage_provider
            .get_entry_args(&EntryArgsRequest {
                author: author.clone(),
                document: Some(document_id.clone()),
            })
            .await
            .unwrap();

        let mut fields = OperationFields::new();
        fields
            .add("username", OperationValue::Text("stitch".to_owned()))
            .unwrap();

        let update_operation = Operation::new_update(
            schema.clone(),
            vec![next_entry_args.entry_hash_backlink.clone().unwrap().into()],
            fields.clone(),
        )
        .unwrap();

        let update_entry = Entry::new(
            &next_entry_args.log_id,
            Some(&update_operation),
            next_entry_args.entry_hash_skiplink.as_ref(),
            next_entry_args.entry_hash_backlink.as_ref(),
            &next_entry_args.seq_num,
        )
        .unwrap();

        let entry_encoded = sign_and_encode(&update_entry, &key_pair).unwrap();
        let operation_encoded = OperationEncoded::try_from(&update_operation).unwrap();
        let doggo_entry = DoggoEntry::new(&entry_encoded, &operation_encoded).unwrap();
        let result = storage_provider.insert_entry(doggo_entry).await;

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn try_insert_non_unique_entry() {
        let storage_provider = test_db().await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let log_id = LogId::new(1);

        let first_entry = storage_provider
            .entry_at_seq_num(&author, &log_id, &SeqNum::new(1).unwrap())
            .await
            .unwrap()
            .unwrap();

        let duplicate_doggo_entry = DoggoEntry::new(
            &first_entry.entry_signed(),
            &first_entry.operation_encoded().unwrap(),
        )
        .unwrap();
        let result = storage_provider.insert_entry(duplicate_doggo_entry).await;

        assert_eq!(result.unwrap_err().to_string(), "Error occured during `EntryStorage` request in storage provider: error returned from database: UNIQUE constraint failed: entries.author, entries.log_id, entries.seq_num")
    }

    #[tokio::test]
    async fn latest_entry() {
        let storage_provider = test_db().await;

        let author_not_in_db = Author::try_from(*KeyPair::new().public_key()).unwrap();
        let log_id = LogId::new(1);

        let latest_entry = storage_provider
            .latest_entry(&author_not_in_db, &log_id)
            .await
            .unwrap();
        assert!(latest_entry.is_none());

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author_in_db = Author::try_from(*key_pair.public_key()).unwrap();

        let latest_entry = storage_provider
            .latest_entry(&author_in_db, &log_id)
            .await
            .unwrap();
        assert_eq!(latest_entry.unwrap().seq_num(), SeqNum::new(100).unwrap());
    }

    #[tokio::test]
    async fn entries_by_schema() {
        let storage_provider = test_db().await;

        let schema_not_in_the_db = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );

        let entries = storage_provider
            .by_schema(&schema_not_in_the_db)
            .await
            .unwrap();
        assert!(entries.is_empty());

        let schema_in_the_db = SchemaId::new(TEST_SCHEMA_ID).unwrap();

        let entries = storage_provider.by_schema(&schema_in_the_db).await.unwrap();
        assert!(entries.len() == 100);
    }

    #[tokio::test]
    async fn entry_by_seq_num() {
        let storage_provider = test_db().await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        for seq_num in [1, 10, 56, 77, 90] {
            let seq_num = SeqNum::new(seq_num).unwrap();
            let entry = storage_provider
                .entry_at_seq_num(&author, &LogId::new(1), &seq_num)
                .await
                .unwrap();
            assert_eq!(entry.unwrap().seq_num(), seq_num)
        }

        let wrong_log = LogId::new(2);
        let entry = storage_provider
            .entry_at_seq_num(&author, &wrong_log, &SeqNum::new(1).unwrap())
            .await
            .unwrap();
        assert!(entry.is_none());

        let author_not_in_db = Author::try_from(*KeyPair::new().public_key()).unwrap();
        let entry = storage_provider
            .entry_at_seq_num(&author_not_in_db, &LogId::new(1), &SeqNum::new(1).unwrap())
            .await
            .unwrap();
        assert!(entry.is_none());

        let seq_num_not_in_log = SeqNum::new(1000).unwrap();
        let entry = storage_provider
            .entry_at_seq_num(&author_not_in_db, &LogId::new(1), &seq_num_not_in_log)
            .await
            .unwrap();
        assert!(entry.is_none())
    }

    #[tokio::test]
    async fn get_entry_by_hash() {
        let storage_provider = test_db().await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        for seq_num in [1, 11, 32, 45, 76] {
            let seq_num = SeqNum::new(seq_num).unwrap();
            let entry = storage_provider
                .entry_at_seq_num(&author, &LogId::new(1), &seq_num)
                .await
                .unwrap()
                .unwrap();

            let entry_hash = entry.hash();
            let entry_by_hash = storage_provider
                .get_entry_by_hash(&entry_hash)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(entry, entry_by_hash)
        }

        let entry_hash_not_in_db = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let entry = storage_provider
            .get_entry_by_hash(&entry_hash_not_in_db)
            .await
            .unwrap();
        assert!(entry.is_none())
    }
}
