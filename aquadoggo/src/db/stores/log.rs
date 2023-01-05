// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::LogStorageError;
use p2panda_rs::storage_provider::traits::LogStore;
use sqlx::{query, query_scalar};

use crate::db::provider::SqlStorage;

/// Implementation of `LogStore` trait which is required when constructing a
/// `StorageProvider`.
///
/// Handles storage and retrieval of logs in the form of `StorageLog` which
/// implements the required `AsStorageLog` trait. An intermediary struct `LogRow`
/// is also used when retrieving a log from the database.
#[async_trait]
impl LogStore for SqlStorage {
    /// Insert a log into storage.
    async fn insert_log(
        &self,
        log_id: &LogId,
        public_key: &PublicKey,
        schema: &SchemaId,
        document: &DocumentId,
    ) -> Result<bool, LogStorageError> {
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
        .bind(public_key.to_string())
        .bind(log_id.as_u64().to_string())
        .bind(document.as_str())
        .bind(schema.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| LogStorageError::Custom(e.to_string()))?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Get a log from storage
    async fn get_log_id(
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
    use p2panda_rs::entry::LogId;
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::operation::OperationId;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::LogStore;
    use p2panda_rs::test_utils::fixtures::{
        public_key, random_document_id, random_operation_id, schema_id,
    };
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    fn prevent_duplicate_log_ids(
        #[from(public_key)] public_key: PublicKey,
        #[from(schema_id)] schema_id: SchemaId,
        #[from(random_document_id)] document: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            assert!(db
                .store
                .insert_log(&LogId::default(), &public_key, &schema_id, &document)
                .await
                .is_ok());
            assert!(db
                .store
                .insert_log(&LogId::default(), &public_key, &schema_id, &document)
                .await
                .is_err());
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

            assert!(db
                .store
                .insert_log(&LogId::default(), &public_key, &schema, &document)
                .await
                .is_ok());
        });
    }

    #[rstest]
    fn latest_log_id(
        #[from(public_key)] public_key: PublicKey,
        #[from(schema_id)] schema_id: SchemaId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let log_id = db.store.latest_log_id(&public_key).await.unwrap();

            assert_eq!(log_id, None);

            for n in 0..12 {
                db.store
                    .insert_log(
                        &LogId::new(n),
                        &public_key,
                        &schema_id,
                        &random_document_id(),
                    )
                    .await
                    .unwrap();

                let log_id = db.store.latest_log_id(&public_key).await.unwrap();
                assert_eq!(Some(LogId::new(n)), log_id);
            }
        });
    }
}
