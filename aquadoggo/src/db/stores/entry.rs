// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use lipmaa_link::get_lipmaa_links_back_to;
use p2panda_rs::entry::decode::decode_entry;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{EncodedEntry, Entry, LogId, SeqNum, Signature};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::EntryStorageError;
use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
use sqlx::{query, query_as};

use crate::db::models::EntryRow;
use crate::db::sql_store::SqlStore;

/// A signed entry and it's encoded operation. Entries are the lowest level data type on the
/// p2panda network, they are signed by authors and form bamboo append only logs. The operation is
/// an entries' payload, it contains the data mutations which authors publish.
///
/// This struct implements the `EntryWithOperation` trait which is required when constructing the
/// `EntryStore`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageEntry {
    /// PublicKey of this entry.
    pub(crate) public_key: PublicKey,

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
    fn public_key(&self) -> &PublicKey {
        &self.public_key
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
        let encoded_entry = EncodedEntry::from_bytes(
            &hex::decode(entry_row.entry_bytes)
                .expect("Decode entry hex entry bytes from database"),
        );
        let entry = decode_entry(&encoded_entry).expect("Decoding encoded entry from database");
        StorageEntry {
            public_key: entry.public_key().to_owned(),
            log_id: entry.log_id().to_owned(),
            seq_num: entry.seq_num().to_owned(),
            skiplink: entry.skiplink().cloned(),
            backlink: entry.backlink().cloned(),
            payload_size: entry.payload_size(),
            payload_hash: entry.payload_hash().to_owned(),
            signature: entry.signature().to_owned(),
            encoded_entry,
            // We unwrap now as all entries currently contain a payload.
            payload: entry_row.payload_bytes.map(|payload| {
                EncodedOperation::from_bytes(
                    &hex::decode(payload).expect("Decode entry payload from database"),
                )
            }),
        }
    }
}

/// Implementation of `EntryStore` trait which is required when constructing a `StorageProvider`.
///
/// Handles storage and retrieval of entries in the form of`StorageEntry` which implements the
/// required `EntryWithOperation` trait. An intermediary struct `EntryRow` is also used when retrieving
/// an entry from the database.
#[async_trait]
impl EntryStore for SqlStore {
    type Entry = StorageEntry;

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
                    public_key,
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
        .bind(entry.public_key().to_string())
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
    async fn get_entry(
        &self,
        hash: &Hash,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                public_key,
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

    /// Get an entry at a sequence position within the log of a public key.
    ///
    /// Returns a result containing the entry wrapped in an option if it was found successfully.
    /// Returns None if the entry was not found in storage. Errors when a fatal storage error
    /// occured.
    async fn get_entry_at_seq_num(
        &self,
        public_key: &PublicKey,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                public_key,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                public_key = $1
                AND log_id = $2
                AND seq_num = $3
            ",
        )
        .bind(public_key.to_string())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row.map(|row| row.into()))
    }

    /// Get the latest entry in the log of a public key.
    ///
    /// Returns a result containing the latest log entry wrapped in an option if an entry was
    /// found. Returns None if the specified public key and log could not be found in storage. Errors
    /// when a fatal storage error occured.
    async fn get_latest_entry(
        &self,
        public_key: &PublicKey,
        log_id: &LogId,
    ) -> Result<Option<StorageEntry>, EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                public_key,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                public_key = $1
                AND log_id = $2
            ORDER BY
                CAST(seq_num AS NUMERIC) DESC
            LIMIT
                1
            ",
        )
        .bind(public_key.to_string())
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
                entries.public_key,
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
                    AND entries.public_key = logs.public_key)
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
        public_key: &PublicKey,
        log_id: &LogId,
        seq_num: &SeqNum,
        max_number_of_entries: usize,
    ) -> Result<Vec<StorageEntry>, EntryStorageError> {
        let max_seq_num = seq_num.as_u64() as usize + max_number_of_entries - 1;
        let entries = query_as::<_, EntryRow>(
            "
            SELECT
                public_key,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                public_key = $1
                AND log_id = $2
                AND CAST(seq_num AS NUMERIC) BETWEEN CAST($3 AS NUMERIC) and CAST($4 AS NUMERIC)
            ORDER BY
                CAST(seq_num AS NUMERIC)
            ",
        )
        .bind(public_key.to_string())
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
        public_key: &PublicKey,
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
                public_key,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                public_key = $1
                AND log_id = $2
                AND CAST(seq_num AS NUMERIC) IN ({})
            ORDER BY
                CAST(seq_num AS NUMERIC) DESC
            ",
            cert_pool_seq_nums
        );

        let entries = query_as::<_, EntryRow>(sql_str.as_str())
            .bind(public_key.to_string())
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
    use p2panda_rs::entry::{EncodedEntry, Entry, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::EncodedOperation;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{EntryStore, EntryWithOperation};
    use p2panda_rs::test_utils::fixtures::{encoded_entry, encoded_operation, entry, random_hash};
    use rstest::rstest;

    use crate::db::stores::test_utils::{doggo_schema, test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    fn insert_entry(
        encoded_entry: EncodedEntry,
        entry: Entry,
        encoded_operation: EncodedOperation,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Insert one entry into the store.
            let result = db
                .store
                .insert_entry(&entry, &encoded_entry, Some(&encoded_operation))
                .await;
            assert!(result.is_ok());

            // Retrieve the entry again by it's hash.
            let retrieved_entry = db
                .store
                .get_entry(&encoded_entry.hash())
                .await
                .expect("Get entry")
                .expect("Unwrap entry");

            // The returned values should match the inserted ones.
            assert_eq!(entry.log_id(), retrieved_entry.log_id());
            assert_eq!(entry.seq_num(), retrieved_entry.seq_num());
            assert_eq!(entry.backlink(), retrieved_entry.backlink());
            assert_eq!(entry.skiplink(), retrieved_entry.skiplink());
            assert_eq!(entry.public_key(), retrieved_entry.public_key());
            assert_eq!(entry.signature(), retrieved_entry.signature());
            assert_eq!(entry.payload_size(), retrieved_entry.payload_size());
            assert_eq!(entry.payload_hash(), retrieved_entry.payload_hash());
            assert_eq!(encoded_entry.hash(), retrieved_entry.hash());
            assert_eq!(
                encoded_operation,
                retrieved_entry.payload().unwrap().to_owned()
            );

            // Convert the retrieved entry back into the types we inserted.
            let retreved_entry: Entry = retrieved_entry.clone().into();
            let retreved_encoded_entry: EncodedEntry = retrieved_entry.into();

            // The types should match.
            assert_eq!(retreved_entry, entry);
            assert_eq!(retreved_encoded_entry, encoded_entry);
        });
    }

    #[rstest]
    fn try_insert_non_unique_entry(
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // The public key of the author who published the entries in the database
            let public_key = db.test_data.key_pairs[0].public_key();

            // We get back the first entry.
            let first_entry = db
                .store
                .get_entry_at_seq_num(&public_key, &LogId::default(), &SeqNum::new(1).unwrap())
                .await
                .expect("Get entry")
                .unwrap();

            // We try to publish it again which should error as entry hashes
            // have a unique constraint.
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
            // The public key of the author who published the entries in the database
            let public_key_in_db = db.test_data.key_pairs[0].public_key();
            // There are no entries assigned to this public key.
            let public_key_not_in_db = KeyPair::new().public_key();
            let log_id = LogId::default();

            // We expect no latest entry by a public key who did not
            // publish to this store at this log yet.
            let latest_entry = db
                .store
                .get_latest_entry(&public_key_not_in_db, &log_id)
                .await
                .expect("Get latest entry for public key and log id");
            assert!(latest_entry.is_none());

            // We expect the latest entry for the requested public key and log.
            let latest_entry = db
                .store
                .get_latest_entry(&public_key_in_db, &log_id)
                .await
                .expect("Get latest entry for public key and log id");
            assert_eq!(latest_entry.unwrap().seq_num(), &SeqNum::new(20).unwrap());

            // If we request for an existing public key but a non-existant log, then we again
            // expect no latest entry.
            let latest_entry = db
                .store
                .get_latest_entry(&public_key_in_db, &LogId::new(1))
                .await
                .expect("Get latest entry for public key and log id");
            assert!(latest_entry.is_none());
        });
    }

    #[rstest]
    fn entries_by_schema(
        #[from(random_hash)] hash: Hash,
        #[from(test_db)]
        #[with(20, 2, 2, false, doggo_schema())]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // No entries were published containing an operation which follows this schema.
            let schema_not_in_the_db = SchemaId::new_application("venue", &hash.into());
            // This schema was used when publishing the test entries.
            let schema_in_the_db = doggo_schema().id().to_owned();

            // Get entries by schema not in db.
            let entries = db
                .store
                .get_entries_by_schema(&schema_not_in_the_db)
                .await
                .expect("Get entries by schema");

            // We expect no entries when the schema has not been used.
            assert!(entries.is_empty());

            // Get entries by schema which is in the db.
            let entries = db
                .store
                .get_entries_by_schema(&schema_in_the_db)
                .await
                .expect("Get entries by schema");

            // Here we expect 80 entries as there are 2 authors each publishing to 2 documents which each contain 20
            // entries.
            //
            // 2 * 2 * 20 = 80
            assert!(entries.len() == 80);
        });
    }

    #[rstest]
    fn entry_by_seq_number(
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // The public key of the author who published the entries to the database
            let public_key = db.test_data.key_pairs[0].public_key();
            let log_id = LogId::default();

            // We should be able to get each entry by it's public_key, log_id and seq_num.
            for seq_num in 1..10 {
                let seq_num = SeqNum::new(seq_num).unwrap();

                // We expect the retrieved entry to match the values we requested.
                let entry = db
                    .store
                    .get_entry_at_seq_num(&public_key, &LogId::default(), &seq_num)
                    .await
                    .expect("Get entry from store")
                    .expect("Optimistically unwrap entry");

                assert_eq!(entry.seq_num(), &seq_num);
                assert_eq!(entry.log_id(), &log_id);
                assert_eq!(entry.public_key(), &public_key);
            }

            // We expect a request to an empty log to return no entries.
            let wrong_log = LogId::new(2);
            let entry = db
                .store
                .get_entry_at_seq_num(&public_key, &wrong_log, &SeqNum::new(1).unwrap())
                .await
                .expect("Get entry from store");
            assert!(entry.is_none());

            // We expect a request to the wrong public key to return no entries.
            let public_key_not_in_db = KeyPair::new().public_key();
            let entry = db
                .store
                .get_entry_at_seq_num(
                    &public_key_not_in_db,
                    &LogId::default(),
                    &SeqNum::new(1).unwrap(),
                )
                .await
                .expect("Get entry from store");
            assert!(entry.is_none());

            // We expect a request for a sequence number which is too high
            // to return no entries.
            let seq_num_not_in_log = SeqNum::new(1000).unwrap();
            let entry = db
                .store
                .get_entry_at_seq_num(
                    &public_key_not_in_db,
                    &LogId::default(),
                    &seq_num_not_in_log,
                )
                .await
                .expect("Get entry from store");
            assert!(entry.is_none());
        });
    }

    #[rstest]
    fn get_entry(
        #[from(test_db)]
        #[with(20, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let public_key = db.test_data.key_pairs[0].public_key();

            // We pick a few entries from the ones in the db to retrieve.
            for seq_num in [1, 11, 18] {
                let seq_num = SeqNum::new(seq_num).unwrap();
                // We get them by their sequence number first.
                let entry = db
                    .store
                    .get_entry_at_seq_num(&public_key, &LogId::default(), &seq_num)
                    .await
                    .unwrap()
                    .unwrap();

                // The we retrieve them by their hash.
                let entry_hash = entry.hash();
                let entry_by_hash = db
                    .store
                    .get_entry(&entry_hash)
                    .await
                    .unwrap()
                    .unwrap();

                // The entries should match.
                assert_eq!(entry, entry_by_hash)
            }

            // If we try to retrieve with a hash of an entry not in the db then
            // we should get none back.
            let entry_hash_not_in_db = random_hash();
            let entry = db
                .store
                .get_entry(&entry_hash_not_in_db)
                .await
                .unwrap();
            assert!(entry.is_none());
        });
    }

    #[rstest]
    #[case(1, 0, 0)]
    #[case(1, 20, 20)]
    #[case(1, 30, 30)]
    #[case(10, 20, 20)]
    #[case(20, 20, 11)]
    #[case(30, 20, 1)]
    #[case(100, 20, 0)]
    fn paginated_log_entries(
        #[case] from: u64,
        #[case] num_of_entries: u64,
        #[case] expected: usize,
        #[from(test_db)]
        // We pre-populate the database with 30 entries for this test.
        #[with(30, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // The public key of the author who published the entries in the db.
            let public_key = db.test_data.key_pairs[0].public_key();

            // Get paginated entries, starting with the one at seq num defined by `from` and
            // continuing for `num_of_entries` or the end of the log is reached.
            let entries = db
                .store
                .get_paginated_log_entries(
                    &public_key,
                    &LogId::default(),
                    &SeqNum::new(from).unwrap(),
                    num_of_entries as usize,
                )
                .await
                .unwrap();

            for entry in &entries {
                // We expect the seq num of every returned entry to lay between the
                // `from` seq num and the `from` + `num_of_entries`
                assert!(
                    entry.seq_num().as_u64() >= from
                        && entry.seq_num().as_u64() <= num_of_entries + from
                )
            }

            // We expect the total number of entries returned to match our `expected`
            // value.
            assert_eq!(entries.len(), expected);
        });
    }

    #[rstest]
    fn get_lipmaa_link_entries(
        #[from(test_db)]
        #[with(20, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // An author with this public key has published to the test database.
            let public_key = db.test_data.key_pairs[0].public_key();

            // Get the certificate pool for a specified public_key, log and seq num.
            let entries = db
                .store
                .get_certificate_pool(&public_key, &LogId::default(), &SeqNum::new(20).unwrap())
                .await
                .unwrap();

            // Convert the seq num of each returned untry into a u64.
            let cert_pool_seq_nums = entries
                .iter()
                .map(|entry| entry.seq_num().as_u64())
                .collect::<Vec<u64>>();

            // The cert pool should match our expected values.
            assert_eq!(cert_pool_seq_nums, vec![19, 18, 17, 13, 4, 1]);
        });
    }
}
