// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::OperationStorageError;
use p2panda_rs::storage_provider::traits::StorageProvider;
use sqlx::query_scalar;

use crate::db::stores::{StorageEntry, StorageLog, StorageOperation};
use crate::db::Pool;
use crate::errors::Result;

/// Sql based storage that implements `StorageProvider`.
#[derive(Clone, Debug)]
pub struct SqlStorage {
    pub(crate) pool: Pool,
}

impl SqlStorage {
    /// Create a new `SqlStorage` using the provided db `Pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// A `StorageProvider` implementation based on `sqlx` that supports SQLite and PostgreSQL
/// databases.
#[async_trait]
impl StorageProvider for SqlStorage {
    type StorageLog = StorageLog;
    type Entry = StorageEntry;
    type Operation = StorageOperation;

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document id by looking up the log
    /// that the entry was stored in.
    async fn get_document_by_entry(&self, entry_hash: &Hash) -> Result<Option<DocumentId>> {
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

        // Unwrap here since we validate hashes before storing them in the db.
        let hash = result.map(|str| {
            Hash::new(&str)
                .expect("Corrupt hash found in database")
                .into()
        });

        Ok(hash)
    }
}

impl SqlStorage {
    /// Returns the schema id for a document view.
    ///
    /// Returns `None` if this document view is not found.
    pub async fn get_schema_by_document_view(
        &self,
        view_id: &DocumentViewId,
    ) -> Result<Option<SchemaId>> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                schema_id
            FROM
                document_views
            WHERE
                document_view_id = $1
            ",
        )
        .bind(view_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Unwrap because we expect no invalid schema ids in the db.
        Ok(result.map(|id_str| id_str.parse().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    // @TODO: bring back test_get_schema_for_view test

    #[rstest]
    fn test_get_schema_for_missing_view(
        random_document_view_id: DocumentViewId,
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let result = db
                .store
                .get_schema_by_document_view(&random_document_view_id)
                .await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        });
    }
}
