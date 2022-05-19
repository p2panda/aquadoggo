// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use lipmaa_link::get_lipmaa_links_back_to;
use p2panda_rs::storage_provider::ValidationError;
use p2panda_rs::Validate;
use sqlx::{query, query_as};

use p2panda_rs::entry::{decode_entry, Entry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{Operation, OperationEncoded};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::EntryStorageError;
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};

use crate::db::models::EntryRow;
use crate::db::provider::SqlStorage;

#[derive(Debug, Clone, PartialEq)]
pub struct StorageEntry {
    entry_signed: EntrySigned,
    operation_encoded: OperationEncoded,
}

impl StorageEntry {
    pub fn entry_decoded(&self) -> Entry {
        // Unwrapping as validation occurs in `EntryWithOperation`.
        decode_entry(self.entry_signed(), self.operation_encoded()).unwrap()
    }

    pub fn entry_signed(&self) -> &EntrySigned {
        &self.entry_signed
    }

    pub fn operation_encoded(&self) -> Option<&OperationEncoded> {
        Some(&self.operation_encoded)
    }
}

impl Validate for StorageEntry {
    type Error = ValidationError;

    fn validate(&self) -> Result<(), Self::Error> {
        self.entry_signed().validate()?;
        if let Some(operation) = self.operation_encoded() {
            operation.validate()?;
        }
        decode_entry(self.entry_signed(), self.operation_encoded())?;
        Ok(())
    }
}

impl From<EntryRow> for StorageEntry {
    fn from(entry_row: EntryRow) -> Self {
        // Unwrapping everything here as we assume values coming from the database are valid.
        let entry_signed = EntrySigned::new(&entry_row.entry_bytes).unwrap();
        let operation_encoded = OperationEncoded::new(&entry_row.payload_bytes.unwrap()).unwrap();
        StorageEntry::new(&entry_signed, &operation_encoded).unwrap()
    }
}

/// Implement `AsStorageEntry` trait for `EntryRow`.
impl AsStorageEntry for StorageEntry {
    type AsStorageEntryError = EntryStorageError;

    fn new(
        entry_signed: &EntrySigned,
        operation_encoded: &OperationEncoded,
    ) -> Result<Self, Self::AsStorageEntryError> {
        let storage_entry = Self {
            entry_signed: entry_signed.clone(),
            operation_encoded: operation_encoded.clone(),
        };

        storage_entry.validate()?;

        Ok(storage_entry)
    }

    fn author(&self) -> Author {
        self.entry_signed.author()
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
        Operation::from(operation_encoded)
    }
}

/// Trait which handles all storage actions relating to `Entries`.
#[async_trait]
impl EntryStore<StorageEntry> for SqlStorage {
    /// Insert an entry into storage.
    ///
    /// Returns an error if the insertion doesn't result in exactly one
    /// affected row.
    async fn insert_entry(&self, entry: StorageEntry) -> Result<(), EntryStorageError> {
        let insert_entry_result = query(
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
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        if insert_entry_result.rows_affected() != 1 {
            return Err(EntryStorageError::Custom(format!(
                "Unexpected number of inserts occured for entry with id: {}",
                entry.hash()
            )));
        }

        Ok(())
    }

    /// Get an entry from storage by it's hash id.
    ///
    /// Returns a result containing the entry wrapped in an option if it was
    /// found successfully. Returns `None` if the entry was not found in storage.
    /// Errors when a fatal storage error occured.
    async fn get_entry_by_hash(
        &self,
        hash: &Hash,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
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
                entry_hash = $1
            ",
        )
        .bind(hash.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row.map(|row| row.into()))
    }

    /// Get an entry at a sequence position within an author's log.
    ///
    /// Returns a result containing the entry wrapped in an option if it was found
    /// successfully. Returns None if the entry was not found in storage. Errors when
    /// a fatal storage error occured.
    async fn get_entry_at_seq_num(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
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

        Ok(entry_row.map(|row| row.into()))
    }

    /// Get the latest entry of an author's log.
    ///
    /// Returns a result containing the latest log entry wrapped in an option if an
    /// entry was found. Returns None if the specified author and log could not be
    /// found in storage. Errors when a fatal storage error occured.
    async fn get_latest_entry(
        &self,
        author: &Author,
        log_id: &LogId,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
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
                CAST(seq_num AS NUMERIC) DESC
            LIMIT
                1
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row.map(|row| row.into()))
    }

    /// Get all entries of a given schema
    ///
    /// Returns a result containing a vector of all entries which follow the passed
    /// schema (identified by it's `SchemaId`). If no entries exist, or the schema
    /// is not known by this node, then an empty vector is returned.
    async fn get_entries_by_schema(
        &self,
        schema: &SchemaId,
    ) -> Result<Vec<StorageEntry>, EntryStorageError> {
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

        Ok(entries.into_iter().map(|row| row.into()).collect())
    }

    /// Get all entries of a given schema.
    ///
    /// Returns a result containing a vector of all entries which follow the passed
    /// schema (identified by it's `SchemaId`). If no entries exist, or the schema
    /// is not known by this node, then an empty vector is returned.
    async fn get_paginated_log_entries(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
        max_number_of_entries: usize,
    ) -> Result<Vec<StorageEntry>, EntryStorageError> {
        let max_seq_num = seq_num.as_u64() as usize + max_number_of_entries - 1;
        let entries = query_as::<_, EntryRow>(
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
                AND CAST(seq_num AS NUMERIC) BETWEEN $3 and $4
            ORDER BY
                CAST(seq_num AS NUMERIC)
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .bind((max_seq_num as u64).to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entries.into_iter().map(|row| row.into()).collect())
    }

    /// Get all entries which make up the certificate pool for a specified entry.
    ///
    /// Returns a result containing a vector of all stored entries which are part
    /// the passed entries' certificate pool. Errors if a fatal storage error
    /// occurs.
    ///
    /// It is worth noting that this method doesn't check if the certificate pool
    /// is complete, it only returns entries which are part of the pool and found
    /// in storage. If an entry was not stored, then the pool may be incomplete.
    async fn get_certificate_pool(
        &self,
        author: &Author,
        log_id: &LogId,
        initial_seq_num: &SeqNum,
    ) -> Result<Vec<StorageEntry>, EntryStorageError> {
        let cert_pool_seq_nums = get_lipmaa_links_back_to(initial_seq_num.as_u64(), 1)
            .iter()
            .map(|seq_num| seq_num.to_string())
            .collect::<Vec<String>>()
            .join(",");

        // Formatting query string in this way as `sqlx` currently
        // doesn't support binding list arguments for IN queries.
        let sql_str = format!(
            "SELECT
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
                AND CAST(seq_num AS NUMERIC) IN ({})
            ORDER BY
                CAST(seq_num AS NUMERIC) DESC
            ",
            cert_pool_seq_nums
        );

        let entries = query_as::<_, EntryRow>(sql_str.as_str())
            .bind(author.as_str())
            .bind(log_id.as_u64().to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entries.into_iter().map(|row| row.into()).collect())
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

    use crate::db::stores::entry::StorageEntry;
    use crate::db::stores::test_utils::test_db;
    use crate::graphql::client::EntryArgsRequest;

    #[tokio::test]
    async fn insert_entry() {
        let storage_provider = test_db(100).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let log_id = LogId::new(1);
        let schema = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        // Derive the document_id by fetching the first entry
        let document_id: DocumentId = storage_provider
            .get_entry_at_seq_num(&author, &log_id, &SeqNum::new(1).unwrap())
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
            vec![next_entry_args.backlink.clone().unwrap().into()],
            fields.clone(),
        )
        .unwrap();

        let update_entry = Entry::new(
            &next_entry_args.log_id,
            Some(&update_operation),
            next_entry_args.skiplink.as_ref(),
            next_entry_args.backlink.as_ref(),
            &next_entry_args.seq_num,
        )
        .unwrap();

        let entry_encoded = sign_and_encode(&update_entry, &key_pair).unwrap();
        let operation_encoded = OperationEncoded::try_from(&update_operation).unwrap();
        let doggo_entry = StorageEntry::new(&entry_encoded, &operation_encoded).unwrap();
        let result = storage_provider.insert_entry(doggo_entry).await;

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn try_insert_non_unique_entry() {
        let storage_provider = test_db(100).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let log_id = LogId::new(1);

        let first_entry = storage_provider
            .get_entry_at_seq_num(&author, &log_id, &SeqNum::new(1).unwrap())
            .await
            .unwrap()
            .unwrap();

        let duplicate_doggo_entry = StorageEntry::new(
            first_entry.entry_signed(),
            first_entry.operation_encoded().unwrap(),
        )
        .unwrap();
        let result = storage_provider.insert_entry(duplicate_doggo_entry).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Error occured during `EntryStorage` request in storage provider: error returned from \
            database: UNIQUE constraint failed: entries.author, entries.log_id, entries.seq_num"
        )
    }

    #[tokio::test]
    async fn latest_entry() {
        let storage_provider = test_db(100).await;

        let author_not_in_db = Author::try_from(*KeyPair::new().public_key()).unwrap();
        let log_id = LogId::new(1);

        let latest_entry = storage_provider
            .get_latest_entry(&author_not_in_db, &log_id)
            .await
            .unwrap();
        assert!(latest_entry.is_none());

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author_in_db = Author::try_from(*key_pair.public_key()).unwrap();

        let latest_entry = storage_provider
            .get_latest_entry(&author_in_db, &log_id)
            .await
            .unwrap();
        assert_eq!(latest_entry.unwrap().seq_num(), SeqNum::new(100).unwrap());
    }

    #[tokio::test]
    async fn entries_by_schema() {
        let storage_provider = test_db(100).await;

        let schema_not_in_the_db = SchemaId::new_application(
            "venue",
            &Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into(),
        );

        let entries = storage_provider
            .get_entries_by_schema(&schema_not_in_the_db)
            .await
            .unwrap();
        assert!(entries.is_empty());

        let schema_in_the_db = SchemaId::new(TEST_SCHEMA_ID).unwrap();

        let entries = storage_provider
            .get_entries_by_schema(&schema_in_the_db)
            .await
            .unwrap();
        assert!(entries.len() == 100);
    }

    #[tokio::test]
    async fn entry_by_seq_num() {
        let storage_provider = test_db(100).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        for seq_num in [1, 10, 56, 77, 90] {
            let seq_num = SeqNum::new(seq_num).unwrap();
            let entry = storage_provider
                .get_entry_at_seq_num(&author, &LogId::new(1), &seq_num)
                .await
                .unwrap();
            assert_eq!(entry.unwrap().seq_num(), seq_num)
        }

        let wrong_log = LogId::new(2);
        let entry = storage_provider
            .get_entry_at_seq_num(&author, &wrong_log, &SeqNum::new(1).unwrap())
            .await
            .unwrap();
        assert!(entry.is_none());

        let author_not_in_db = Author::try_from(*KeyPair::new().public_key()).unwrap();
        let entry = storage_provider
            .get_entry_at_seq_num(&author_not_in_db, &LogId::new(1), &SeqNum::new(1).unwrap())
            .await
            .unwrap();
        assert!(entry.is_none());

        let seq_num_not_in_log = SeqNum::new(1000).unwrap();
        let entry = storage_provider
            .get_entry_at_seq_num(&author_not_in_db, &LogId::new(1), &seq_num_not_in_log)
            .await
            .unwrap();
        assert!(entry.is_none())
    }

    #[tokio::test]
    async fn get_entry_by_hash() {
        let storage_provider = test_db(100).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        for seq_num in [1, 11, 32, 45, 76] {
            let seq_num = SeqNum::new(seq_num).unwrap();
            let entry = storage_provider
                .get_entry_at_seq_num(&author, &LogId::new(1), &seq_num)
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

    #[tokio::test]
    async fn get_paginated_log_entries() {
        let storage_provider = test_db(50).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        let entries = storage_provider
            .get_paginated_log_entries(&author, &LogId::default(), &SeqNum::default(), 20)
            .await
            .unwrap();
        for entry in entries.clone() {
            assert!(entry.seq_num().as_u64() >= 1 && entry.seq_num().as_u64() <= 20)
        }
        assert_eq!(entries.len(), 20);
    }

    #[tokio::test]
    async fn gets_all_skiplink_entries_for_entry() {
        let storage_provider = test_db(50).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(*key_pair.public_key()).unwrap();

        let entries = storage_provider
            .get_certificate_pool(&author, &LogId::default(), &SeqNum::new(20).unwrap())
            .await
            .unwrap();

        let cert_pool_seq_nums = entries
            .iter()
            .map(|entry| entry.seq_num().as_u64())
            .collect::<Vec<u64>>();

        assert!(!entries.is_empty());
        assert_eq!(cert_pool_seq_nums, vec![19, 18, 17, 13, 4, 1]);
    }
}
