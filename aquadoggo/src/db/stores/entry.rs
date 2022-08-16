// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use lipmaa_link::get_lipmaa_links_back_to;
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{EncodedEntry, Entry, LogId, SeqNum, Signature};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::EntryStorageError;
use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
use sqlx::{query, query_as};

use crate::db::models::EntryRow;
use crate::db::provider::SqlStorage;

/// A signed entry and it's encoded operation. Entries are the lowest level data type on the
/// p2panda network, they are signed by authors and form bamboo append only logs. The operation is
/// an entries' payload, it contains the data mutations which authors publish.
///
/// This struct implements the `EntryWithOperation` trait which is required when constructing the
/// `EntryStore`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageEntry {
    /// Author of this entry.
    pub(crate) author: Author,

    /// Used log for this entry.
    pub(crate) log_id: LogId,

    /// Sequence number of this entry.
    pub(crate) seq_num: SeqNum,

    /// Hash of skiplink Bamboo entry.
    pub(crate) skiplink: Option<Hash>,

    /// Hash of previous Bamboo entry.
    pub(crate) backlink: Option<Hash>,

    /// Byte size of payload.
    pub(crate) payload_size: u64,

    /// Hash of payload.
    pub(crate) payload_hash: Hash,

    /// Ed25519 signature of entry.
    pub(crate) signature: Signature,

    /// Encoded entry bytes.
    pub(crate) encoded_entry: EncodedEntry,

    /// Encoded entry bytes.
    pub(crate) payload: Option<EncodedOperation>,
}

impl EntryWithOperation for StorageEntry {
    fn payload(&self) -> Option<&EncodedOperation> {
        self.payload.as_ref()
    }
}

impl AsEntry for StorageEntry {
    /// Returns public key of entry.
    fn public_key(&self) -> &Author {
        &self.author
    }

    /// Returns log id of entry.
    fn log_id(&self) -> &LogId {
        &self.log_id
    }

    /// Returns sequence number of entry.
    fn seq_num(&self) -> &SeqNum {
        &self.seq_num
    }

    /// Returns hash of skiplink entry when given.
    fn skiplink(&self) -> Option<&Hash> {
        self.skiplink.as_ref()
    }

    /// Returns hash of backlink entry when given.
    fn backlink(&self) -> Option<&Hash> {
        self.backlink.as_ref()
    }

    /// Returns payload size of operation.
    fn payload_size(&self) -> u64 {
        self.payload_size
    }

    /// Returns payload hash of operation.
    fn payload_hash(&self) -> &Hash {
        &self.payload_hash
    }

    /// Returns signature of entry.
    fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl AsEncodedEntry for StorageEntry {
    /// Generates and returns hash of encoded entry.
    fn hash(&self) -> Hash {
        self.encoded_entry.hash()
    }

    /// Returns entry as bytes.
    fn into_bytes(&self) -> Vec<u8> {
        self.encoded_entry.into_bytes()
    }

    /// Returns payload size (number of bytes) of total encoded entry.
    fn size(&self) -> u64 {
        self.encoded_entry.size()
    }
}

/// `From` implementation for converting an `EntryRow` into a `StorageEntry`. This is needed when
/// retrieving entries from the database. The `sqlx` crate coerces returned entry rows into
/// `EntryRow` but we want them as `StorageEntry` which contains typed values.
impl From<EntryRow> for StorageEntry {
    fn from(entry_row: EntryRow) -> Self {
        // @TODO: It's ridiculous we need to decode the entry from the bytes when we access it
        // like this, we should rather store every entry value in the table. However I don't
        // want to change that as part of this PR so I write this sad note and raise an issue
        // later.
        let encoded_entry = EncodedEntry::from_str(&entry_row.entry_bytes);
        let entry = decode_entry(&encoded_entry).unwrap();
        StorageEntry {
            author: entry.public_key().to_owned(),
            log_id: entry.log_id().to_owned(),
            seq_num: entry.seq_num().to_owned(),
            skiplink: entry.skiplink().cloned(),
            backlink: entry.backlink().cloned(),
            payload_size: entry.payload_size(),
            payload_hash: entry.payload_hash().to_owned(),
            signature: entry.signature().to_owned(),
            encoded_entry,
            // We unwrap now as all entries currently contain a payload.
            payload: entry_row
                .payload_bytes
                .map(|payload| EncodedOperation::new(payload.as_bytes())),
        }
    }
}

/// Implementation of `EntryStore` trait which is required when constructing a `StorageProvider`.
///
/// Handles storage and retrieval of entries in the form of`StorageEntry` which implements the
/// required `EntryWithOperation` trait. An intermediary struct `EntryRow` is also used when retrieving
/// an entry from the database.
#[async_trait]
impl EntryStore<StorageEntry> for SqlStorage {
    /// Insert an entry into storage.
    ///
    /// Returns an error if the insertion doesn't result in exactly one
    /// affected row.
    async fn insert_entry(
        &self,
        entry: &Entry,
        encoded_entry: &EncodedEntry,
        encoded_operation: Option<&EncodedOperation>,
    ) -> Result<(), EntryStorageError> {
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
        .bind(entry.public_key().as_str())
        .bind(encoded_entry.into_hex())
        .bind(encoded_entry.hash().as_str())
        .bind(entry.log_id().as_u64().to_string())
        .bind(encoded_operation.map(|payload| payload.to_string()))
        .bind(entry.payload_hash().as_str())
        .bind(entry.seq_num().as_u64().to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        if insert_entry_result.rows_affected() != 1 {
            return Err(EntryStorageError::Custom(format!(
                "Unexpected number of inserts occured for entry with id: {}",
                encoded_entry.hash()
            )));
        }

        Ok(())
    }

    /// Get an entry from storage by it's hash id.
    ///
    /// Returns a result containing the entry wrapped in an option if it was found successfully.
    /// Returns `None` if the entry was not found in storage. Errors when a fatal storage error
    /// occured.
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
    /// Returns a result containing the entry wrapped in an option if it was found successfully.
    /// Returns None if the entry was not found in storage. Errors when a fatal storage error
    /// occured.
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
    /// Returns a result containing the latest log entry wrapped in an option if an entry was
    /// found. Returns None if the specified author and log could not be found in storage. Errors
    /// when a fatal storage error occured.
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
    /// Returns a result containing a vector of all entries which follow the passed schema
    /// (identified by it's `SchemaId`). If no entries exist, or the schema is not known by this
    /// node, then an empty vector is returned.
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
        .bind(schema.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entries.into_iter().map(|row| row.into()).collect())
    }

    /// Get all entries of a given schema.
    ///
    /// Returns a result containing a vector of all entries which follow the passed schema
    /// (identified by it's `SchemaId`). If no entries exist, or the schema is not known by this
    /// node, then an empty vector is returned.
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
                AND CAST(seq_num AS NUMERIC) BETWEEN CAST($3 AS NUMERIC) and CAST($4 AS NUMERIC)
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
    /// Returns a result containing a vector of all stored entries which are part the passed
    /// entries' certificate pool. Errors if a fatal storage error occurs.
    ///
    /// It is worth noting that this method doesn't check if the certificate pool is complete, it
    /// only returns entries which are part of the pool and found in storage. If an entry was not
    /// stored, then the pool may be incomplete.
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
    use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
    use p2panda_rs::test_utils::constants;
    use p2panda_rs::test_utils::fixtures::random_hash;
    use rstest::rstest;

    use crate::db::stores::test_utils::{doggo_schema, test_db, TestDatabase, TestDatabaseRunner};

    // @TODO: bring back insert_entry test

    #[rstest]
    fn try_insert_non_unique_entry(
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::from(db.test_data.key_pairs[0].public_key());
            let log_id = LogId::default();

            let first_entry = db
                .store
                .get_entry_at_seq_num(&author, &log_id, &SeqNum::new(1).unwrap())
                .await
                .unwrap()
                .unwrap();

            let result = db
                .store
                .insert_entry(
                    &first_entry.clone().into(),
                    &first_entry.clone().into(),
                    first_entry.payload(),
                )
                .await;
            assert!(result.is_err());
        });
    }

    #[rstest]
    fn latest_entry(
        #[from(test_db)]
        #[with(20, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author_not_in_db = Author::from(KeyPair::new().public_key());
            let log_id = LogId::default();

            let latest_entry = db
                .store
                .get_latest_entry(&author_not_in_db, &log_id)
                .await
                .unwrap();
            assert!(latest_entry.is_none());

            let author_in_db = Author::from(db.test_data.key_pairs[0].public_key());

            let latest_entry = db
                .store
                .get_latest_entry(&author_in_db, &log_id)
                .await
                .unwrap();
            assert_eq!(latest_entry.unwrap().seq_num(), &SeqNum::new(20).unwrap());
        });
    }

    #[rstest]
    fn entries_by_schema(
        #[from(random_hash)] hash: Hash,
        #[from(test_db)]
        #[with(20, 2, 1, false, doggo_schema())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let schema_not_in_the_db = SchemaId::new_application("venue", &hash.into());

            let entries = db
                .store
                .get_entries_by_schema(&schema_not_in_the_db)
                .await
                .unwrap();
            assert!(entries.is_empty());

            let entries = db
                .store
                .get_entries_by_schema(doggo_schema().id())
                .await
                .unwrap();
            println!("{}", entries.len());
            assert!(entries.len() == 40);
        });
    }

    #[rstest]
    fn entry_by_seq_number(
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::from(db.test_data.key_pairs[0].public_key());

            for seq_num in 1..10 {
                let seq_num = SeqNum::new(seq_num).unwrap();
                let entry = db
                    .store
                    .get_entry_at_seq_num(&author, &LogId::default(), &seq_num)
                    .await
                    .unwrap();
                assert_eq!(entry.unwrap().seq_num(), &seq_num)
            }

            let wrong_log = LogId::new(2);
            let entry = db
                .store
                .get_entry_at_seq_num(&author, &wrong_log, &SeqNum::new(1).unwrap())
                .await
                .unwrap();
            assert!(entry.is_none());

            let author_not_in_db = Author::from(KeyPair::new().public_key());
            let entry = db
                .store
                .get_entry_at_seq_num(
                    &author_not_in_db,
                    &LogId::default(),
                    &SeqNum::new(1).unwrap(),
                )
                .await
                .unwrap();
            assert!(entry.is_none());

            let seq_num_not_in_log = SeqNum::new(1000).unwrap();
            let entry = db
                .store
                .get_entry_at_seq_num(&author_not_in_db, &LogId::default(), &seq_num_not_in_log)
                .await
                .unwrap();
            assert!(entry.is_none());
        });
    }

    #[rstest]
    fn get_entry_by_hash(
        #[from(test_db)]
        #[with(20, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::from(db.test_data.key_pairs[0].public_key());

            for seq_num in [1, 11, 18] {
                let seq_num = SeqNum::new(seq_num).unwrap();
                let entry = db
                    .store
                    .get_entry_at_seq_num(&author, &LogId::default(), &seq_num)
                    .await
                    .unwrap()
                    .unwrap();

                let entry_hash = entry.hash();
                let entry_by_hash = db
                    .store
                    .get_entry_by_hash(&entry_hash)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(entry, entry_by_hash)
            }

            let entry_hash_not_in_db = random_hash();
            let entry = db
                .store
                .get_entry_by_hash(&entry_hash_not_in_db)
                .await
                .unwrap();
            assert!(entry.is_none());
        });
    }

    #[rstest]
    fn paginated_log_entries(
        #[from(test_db)]
        #[with(30, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::from(db.test_data.key_pairs[0].public_key());

            let entries = db
                .store
                .get_paginated_log_entries(&author, &LogId::default(), &SeqNum::default(), 20)
                .await
                .unwrap();

            for entry in entries.clone() {
                assert!(entry.seq_num().as_u64() >= 1 && entry.seq_num().as_u64() <= 20)
            }

            assert_eq!(entries.len(), 20);

            let entries = db
                .store
                .get_paginated_log_entries(
                    &author,
                    &LogId::default(),
                    &SeqNum::new(21).unwrap(),
                    20,
                )
                .await
                .unwrap();

            assert_eq!(entries.len(), 10);
        });
    }

    #[rstest]
    fn get_lipmaa_link_entries(
        #[from(test_db)]
        #[with(20, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::from(db.test_data.key_pairs[0].public_key());

            let entries = db
                .store
                .get_certificate_pool(&author, &LogId::default(), &SeqNum::new(20).unwrap())
                .await
                .unwrap();

            let cert_pool_seq_nums = entries
                .iter()
                .map(|entry| entry.seq_num().as_u64())
                .collect::<Vec<u64>>();

            assert!(!entries.is_empty());
            assert_eq!(cert_pool_seq_nums, vec![19, 18, 17, 13, 4, 1]);
        });
    }
}
