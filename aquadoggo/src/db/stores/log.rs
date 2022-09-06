// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::LogStorageError;
use p2panda_rs::storage_provider::traits::{AsStorageLog, LogStore};
use sqlx::{query, query_scalar};

use crate::db::provider::SqlStorage;

/// Tracks the assigment of an public_key's logs to documents and records their schema.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
///
/// `StorageLog` implements the trait `AsStorageLog` which is required when defining a `LogStore`.
#[derive(Debug)]
pub struct StorageLog {
    public_key: PublicKey,
    log_id: LogId,
    document_id: DocumentId,
    schema_id: SchemaId,
}

impl AsStorageLog for StorageLog {
    fn new(
        public_key: &PublicKey,
        schema_id: &SchemaId,
        document_id: &DocumentId,
        log_id: &LogId,
    ) -> Self {
        Self {
            public_key: public_key.to_owned(),
            log_id: log_id.to_owned(),
            document_id: document_id.to_owned(),
            schema_id: schema_id.to_owned(),
        }
    }

    fn public_key(&self) -> PublicKey {
        self.public_key.clone()
    }

    fn id(&self) -> LogId {
        self.log_id
    }

    fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    fn schema_id(&self) -> SchemaId {
        self.schema_id.clone()
    }
}

/// Implementation of `LogStore` trait which is required when constructing a
/// `StorageProvider`.
///
/// Handles storage and retrieval of logs in the form of `StorageLog` which
/// implements the required `AsStorageLog` trait. An intermediary struct `LogRow`
/// is also used when retrieving a log from the database.
#[async_trait]
impl LogStore<StorageLog> for SqlStorage {
    /// Insert a log into storage.
    async fn insert_log(&self, log: StorageLog) -> Result<bool, LogStorageError> {
        let rows_affected = query(
            "
            INSERT INTO
                logs (
                    public_key,
                    log_id,
                    document,
                    schema
                )
            VALUES
                ($1, $2, $3, $4)
            ",
        )
        .bind(log.public_key().to_string())
        .bind(log.id().as_u64().to_string())
        .bind(log.document_id().as_str())
        .bind(log.schema_id().to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| LogStorageError::Custom(e.to_string()))?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Get a log from storage
    async fn get(
        &self,
        public_key: &PublicKey,
        document_id: &DocumentId,
    ) -> Result<Option<LogId>, LogStorageError> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                public_key = $1
                AND document = $2
            ",
        )
        .bind(public_key.to_string())
        .bind(document_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| LogStorageError::Custom(e.to_string()))?;

        // Wrap u64 inside of `P2PandaLog` instance
        let log_id: Option<LogId> = result.map(|str| {
            str.parse()
                .unwrap_or_else(|_| panic!("Corrupt u64 integer found in database: '{0}'", &str))
        });

        Ok(log_id)
    }

    /// Determines the next unused log_id of an public_key.
    ///
    /// @TODO: This will be deprecated as functionality is replaced by
    /// `latest_log_id + validated next log id methods.
    async fn next_log_id(&self, public_key: &PublicKey) -> Result<LogId, LogStorageError> {
        // Get all log ids from this public_key
        let mut result: Vec<String> = query_scalar(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                public_key = $1
            ",
        )
        .bind(public_key.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| LogStorageError::Custom(e.to_string()))?;

        // Convert all strings representing u64 integers to `LogId` instances
        let mut log_ids: Vec<LogId> = result
            .iter_mut()
            .map(|str| {
                str.parse().unwrap_or_else(|_| {
                    panic!("Corrupt u64 integer found in database: '{0}'", &str)
                })
            })
            .collect();

        // The log id selection below expects log ids in sorted order. We can't easily use SQL
        // for this because log IDs are stored as `VARCHAR`, which doesn't sort numbers correctly.
        // A good solution would not require reading all existing log ids to find the next
        // available one. See this issue: https://github.com/p2panda/aquadoggo/issues/67
        log_ids.sort();

        // Find next unused document log by comparing the sequence of known log ids with an
        // sequence of subsequent log ids until we find a gap.
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Success! Found unused log id
            if next_log_id != *log_id {
                break;
            }

            // Otherwise, try next possible log id
            next_log_id = match next_log_id.next() {
                Some(log_id) => Ok(log_id),
                None => Err(LogStorageError::Custom("Max log id reached".to_string())),
            }?;
        }

        Ok(next_log_id)
    }

    /// Determines the latest `LogId` of an public_key.
    ///
    /// Returns either the highest known `LogId` for an public_key or `None` if no logs are known from
    /// the passed public_key.
    async fn latest_log_id(
        &self,
        public_key: &PublicKey,
    ) -> Result<Option<LogId>, LogStorageError> {
        // Get all log ids from this public_key
        let result: Option<String> = query_scalar(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                public_key = $1
            ORDER BY
                CAST(log_id AS NUMERIC) DESC LIMIT 1
            ",
        )
        .bind(public_key.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| LogStorageError::Custom(e.to_string()))?;

        // Convert string representing u64 integers to `LogId` instance
        let log_id: Option<LogId> = result.map(|str| {
            str.parse()
                .unwrap_or_else(|_| panic!("Corrupt u64 integer found in database: '{0}'", &str))
        });

        Ok(log_id)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::decode::decode_entry;
    use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
    use p2panda_rs::entry::{EncodedEntry, LogId};
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::operation::OperationId;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{
        AsStorageLog, EntryStore, LogStore, StorageProvider,
    };
    use p2panda_rs::test_utils::fixtures::{
        encoded_entry, public_key, random_document_id, random_operation_id, schema_id,
    };
    use rstest::rstest;

    use crate::db::stores::log::StorageLog;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    fn prevent_duplicate_log_ids(
        #[from(public_key)] public_key: PublicKey,
        #[from(schema_id)] schema_id: SchemaId,
        #[from(random_document_id)] document: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let log = StorageLog::new(
                &public_key,
                &schema_id,
                &document.clone(),
                &LogId::default(),
            );
            assert!(db.store.insert_log(log).await.is_ok());

            let log = StorageLog::new(&public_key, &schema_id, &document, &LogId::default());
            assert!(db.store.insert_log(log).await.is_err());
        });
    }

    #[rstest]
    fn with_multi_hash_schema_id(
        #[from(public_key)] public_key: PublicKey,
        #[from(random_operation_id)] operation_id_1: OperationId,
        #[from(random_operation_id)] operation_id_2: OperationId,
        #[from(random_document_id)] document: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let schema = SchemaId::new_application(
                "venue",
                &DocumentViewId::new(&[operation_id_1, operation_id_2]),
            );

            let log = StorageLog::new(&public_key, &schema, &document, &LogId::default());

            assert!(db.store.insert_log(log).await.is_ok());
        });
    }

    #[rstest]
    fn latest_log_id(
        #[from(public_key)] public_key: PublicKey,
        #[from(schema_id)] schema_id: SchemaId,
        #[from(test_db)] runner: TestDatabaseRunner,
        #[from(random_document_id)] document_id: DocumentId,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let log_id = db.store.latest_log_id(&public_key).await.unwrap();

            assert_eq!(log_id, None);

            for n in 0..12 {
                let log = StorageLog::new(&public_key, &schema_id, &document_id, &LogId::new(n));
                db.store.insert_log(log).await.unwrap();

                let log_id = db.store.latest_log_id(&public_key).await.unwrap();
                assert_eq!(Some(LogId::new(n)), log_id);
            }
        });
    }

    #[rstest]
    fn document_log_id(
        #[from(schema_id)] schema_id: SchemaId,
        #[from(encoded_entry)] encoded_entry: EncodedEntry,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Expect database to return nothing yet
            assert_eq!(
                db.store
                    .get_document_by_entry(&encoded_entry.hash())
                    .await
                    .unwrap(),
                None
            );

            let entry = decode_entry(&encoded_entry).unwrap();
            let public_key = entry.public_key();
            // Store entry in database
            assert!(db
                .store
                .insert_entry(&entry, &encoded_entry, None)
                .await
                .is_ok());

            let log = StorageLog::new(
                public_key,
                &schema_id,
                &encoded_entry.hash().into(),
                &LogId::default(),
            );

            // Store log in database
            assert!(db.store.insert_log(log).await.is_ok());

            // Expect to find document id in database. The document id should be the same as the
            // hash of the first entry in the log.
            assert_eq!(
                db.store
                    .get_document_by_entry(&encoded_entry.hash())
                    .await
                    .unwrap(),
                Some(encoded_entry.hash().into())
            );
        });
    }

    #[rstest]
    fn log_ids(
        #[from(public_key)] public_key: PublicKey,
        #[from(test_db)] runner: TestDatabaseRunner,
        #[from(schema_id)] schema_id: SchemaId,
        #[from(random_document_id)] document_first: DocumentId,
        #[from(random_document_id)] document_second: DocumentId,
        #[from(random_document_id)] document_third: DocumentId,
        #[from(random_document_id)] document_forth: DocumentId,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Register two log ids at the beginning
            let log_1 =
                StorageLog::new(&public_key, &schema_id, &document_first, &LogId::default());
            let log_2 = StorageLog::new(&public_key, &schema_id, &document_second, &LogId::new(1));

            db.store.insert_log(log_1).await.unwrap();
            db.store.insert_log(log_2).await.unwrap();

            // Find next free log id and register it
            let log_id = db.store.next_log_id(&public_key).await.unwrap();
            assert_eq!(log_id, LogId::new(2));

            let log_3 = StorageLog::new(&public_key, &schema_id, &document_third, &log_id);

            db.store.insert_log(log_3).await.unwrap();

            // Find next free log id and register it
            let log_id = db.store.next_log_id(&public_key).await.unwrap();
            assert_eq!(log_id, LogId::new(3));

            let log_4 = StorageLog::new(&public_key, &schema_id, &document_forth, &log_id);

            db.store.insert_log(log_4).await.unwrap();

            // Find next free log id
            let log_id = db.store.next_log_id(&public_key).await.unwrap();
            assert_eq!(log_id, LogId::new(4));
        });
    }
}
