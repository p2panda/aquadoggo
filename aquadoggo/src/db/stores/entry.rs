// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::vec;

use async_trait::async_trait;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{EncodedEntry, Entry, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::storage_provider::error::EntryStorageError;
use p2panda_rs::storage_provider::traits::EntryStore;
use sqlx::{query, query_as};

use crate::db::models::{EntryRow, LogHeightRow};
use crate::db::types::StorageEntry;
use crate::db::SqlStore;

/// Implementation of `EntryStore` trait which is required when constructing a `StorageProvider`.
///
/// Handles storage and retrieval of entries in the form of `StorageEntry`. An intermediary struct
/// `EntryRow` is used when retrieving an entry from the database.
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

    /// Get an entry from storage by its hash id.
    ///
    /// Returns a result containing the entry wrapped in an option if it was found successfully.
    /// Returns `None` if the entry was not found in storage. Errors when a fatal storage error
    /// occured.
    async fn get_entry(&self, hash: &Hash) -> Result<Option<StorageEntry>, EntryStorageError> {
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
}

impl SqlStore {
    pub async fn get_document_log_heights(
        &self,
        document_ids: &[DocumentId],
    ) -> Result<Vec<(PublicKey, Vec<(LogId, SeqNum)>)>, EntryStorageError> {
        // If no document ids were passed then don't query the database. Instead return an empty
        // vec now already.
        if document_ids.is_empty() {
            return Ok(vec![]);
        }

        let document_ids_str: String = document_ids
            .iter()
            .map(|document_id| format!("'{}'", document_id.as_str()))
            .collect::<Vec<String>>()
            .join(", ");

        let log_height_rows = query_as::<_, LogHeightRow>(&format!(
            "
            SELECT
                entries.public_key,
                entries.log_id,
                CAST(MAX(CAST(entries.seq_num AS NUMERIC)) AS TEXT) as seq_num
            FROM
                entries
            INNER JOIN logs
                ON entries.log_id = logs.log_id
                    AND entries.public_key = logs.public_key
            WHERE
                logs.document IN ({document_ids_str})
            GROUP BY
                entries.public_key, entries.log_id
            ORDER BY
                entries.public_key, CAST(entries.log_id AS NUMERIC)
            ",
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        let mut log_heights = HashMap::<PublicKey, Vec<(LogId, SeqNum)>>::new();

        // Aggregate log height rows into a map.
        for LogHeightRow {
            public_key,
            log_id,
            seq_num,
        } in log_height_rows
        {
            let public_key: PublicKey = public_key
                .parse()
                .expect("Values stored in the database are valid");
            let log_id: LogId = log_id
                .parse()
                .expect("Values stored in the database are valid");
            let seq_num: SeqNum = seq_num
                .parse()
                .expect("Values stored in the database are valid");

            if let Some(author_logs) = log_heights.get_mut(&public_key) {
                author_logs.push((log_id, seq_num));
            } else {
                let author_logs = vec![(log_id, seq_num)];
                log_heights.insert(public_key, author_logs);
            }
        }

        // Convert log heights map back into vec.
        Ok(log_heights.into_iter().collect())
    }

    pub async fn get_entries_from(
        &self,
        public_key: &PublicKey,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Vec<StorageEntry>, EntryStorageError> {
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
                AND CAST(seq_num AS NUMERIC) >= CAST($3 AS NUMERIC)
            ORDER BY
                CAST(seq_num AS NUMERIC)
            ",
        )
        .bind(public_key.to_string())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EntryStorageError::Custom(e.to_string()))?;

        Ok(entries.into_iter().map(|row| row.into()).collect())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
    use p2panda_rs::entry::{EncodedEntry, Entry, EntryBuilder, LogId, SeqNum};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::EncodedOperation;
    use p2panda_rs::storage_provider::traits::EntryStore;
    use p2panda_rs::test_utils::fixtures::{encoded_entry, encoded_operation, entry, random_hash};
    use rstest::rstest;

    use crate::test_utils::{
        populate_store_config, populate_store, test_runner, PopulateStoreConfig, TestNode,
    };

    #[rstest]
    fn insert_entry(
        encoded_entry: EncodedEntry,
        entry: Entry,
        encoded_operation: EncodedOperation,
    ) {
        test_runner(|node: TestNode| async move {
            // Insert one entry into the store.
            let result = node
                .context
                .store
                .insert_entry(&entry, &encoded_entry, Some(&encoded_operation))
                .await;
            assert!(result.is_ok());

            // Retrieve the entry again by its hash
            let retrieved_entry = node
                .context
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
            assert_eq!(retrieved_entry.encoded_entry, encoded_entry);
        });
    }

    #[rstest]
    fn try_insert_non_unique_entry(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let _ = populate_store(&node.context.store, &config).await;

            // The key pair of the author who published to the note.
            let key_pair = config.authors.get(0).expect("At least one key pair");

            // We get back the first entry.
            let first_entry = node
                .context
                .store
                .get_entry_at_seq_num(
                    &key_pair.public_key(),
                    &LogId::default(),
                    &SeqNum::new(1).unwrap(),
                )
                .await
                .expect("Get entry")
                .unwrap();

            // Construct a new entry from it with the same values.
            let entry = EntryBuilder::new()
                .sign(first_entry.payload().unwrap(), key_pair)
                .unwrap();

            // We try to publish it again which should error as entry hashes
            // have a unique constraint.
            let result = node
                .context
                .store
                .insert_entry(&entry, &first_entry.encoded_entry, first_entry.payload())
                .await;

            assert!(result.is_err());
        });
    }

    #[rstest]
    fn latest_entry(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let _ = populate_store(&node.context.store, &config).await;

            // The public key of the author who published to the node.
            let public_key_in_db = config
                .authors
                .get(0)
                .expect("At least one key pair")
                .public_key();

            // There are no entries assigned to this public key.
            let public_key_not_in_db = KeyPair::new().public_key();

            // We expect no latest entry by a public key who did not
            // publish to this store at this log yet.
            let latest_entry = node
                .context
                .store
                .get_latest_entry(&public_key_not_in_db, &LogId::default())
                .await
                .expect("Get latest entry for public key and log id");
            assert!(latest_entry.is_none());

            // We expect the latest entry for the requested public key and log.
            let latest_entry = node
                .context
                .store
                .get_latest_entry(&public_key_in_db, &LogId::default())
                .await
                .expect("Get latest entry for public key and log id");
            assert_eq!(latest_entry.unwrap().seq_num(), &SeqNum::new(2).unwrap());

            // If we request for an existing public key but a non-existant log, then we again
            // expect no latest entry.
            let latest_entry = node
                .context
                .store
                .get_latest_entry(&public_key_in_db, &LogId::new(1))
                .await
                .expect("Get latest entry for public key and log id");
            assert!(latest_entry.is_none());
        });
    }

    #[rstest]
    fn entry_by_seq_number(
        #[from(populate_store_config)]
        #[with(10, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let _ = populate_store(&node.context.store, &config).await;
            // The public key of the author who published to the node.
            let public_key = config
                .authors
                .get(0)
                .expect("At least one key pair")
                .public_key();

            // We should be able to get each entry by its public_key, log_id and seq_num.
            for seq_num in 1..10 {
                let seq_num = SeqNum::new(seq_num).unwrap();

                // We expect the retrieved entry to match the values we requested.
                let entry = node
                    .context
                    .store
                    .get_entry_at_seq_num(&public_key, &LogId::default(), &seq_num)
                    .await
                    .expect("Get entry from store")
                    .expect("Optimistically unwrap entry");

                assert_eq!(entry.seq_num(), &seq_num);
                assert_eq!(entry.log_id(), &LogId::default());
                assert_eq!(entry.public_key(), &public_key);
            }

            // We expect a request to an empty log to return no entries.
            let wrong_log = LogId::new(2);
            let entry = node
                .context
                .store
                .get_entry_at_seq_num(&public_key, &wrong_log, &SeqNum::new(1).unwrap())
                .await
                .expect("Get entry from store");
            assert!(entry.is_none());

            // We expect a request to the wrong public key to return no entries.
            let public_key_not_in_db = KeyPair::new().public_key();
            let entry = node
                .context
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
            let entry = node
                .context
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
        #[from(populate_store_config)]
        #[with(20, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let _ = populate_store(&node.context.store, &config).await;

            // The public key of the author who published to the node.
            let public_key = config
                .authors
                .get(0)
                .expect("At least one key pair")
                .public_key();

            // We pick a few entries from the ones in the db to retrieve.
            for seq_num in [1, 11, 18] {
                let seq_num = SeqNum::new(seq_num).unwrap();
                // We get them by their sequence number first.
                let entry = node
                    .context
                    .store
                    .get_entry_at_seq_num(&public_key, &LogId::default(), &seq_num)
                    .await
                    .unwrap()
                    .unwrap();

                // The we retrieve them by their hash.
                let entry_hash = entry.hash();
                let entry_by_hash = node
                    .context
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
            let entry = node
                .context
                .store
                .get_entry(&entry_hash_not_in_db)
                .await
                .unwrap();
            assert!(entry.is_none());
        });
    }

    #[rstest]
    fn get_entries_from(
        #[from(populate_store_config)]
        #[with(20, 2, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let _ = populate_store(&node.context.store, &config).await;
            let public_key = config.authors[0].public_key();
            let entries = node
                .context
                .store
                .get_entries_from(&public_key, &LogId::default(), &SeqNum::new(10).unwrap())
                .await
                .unwrap();

            assert_eq!(entries.len(), 11);
        });
    }
}
