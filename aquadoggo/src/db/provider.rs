// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::VerifiedOperation;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::OperationStorageError;
use p2panda_rs::storage_provider::traits::StorageProvider;
use sqlx::query_scalar;

use crate::db::request::{EntryArgsRequest, PublishEntryRequest};
use crate::db::stores::{StorageEntry, StorageLog};
use crate::db::Pool;
use crate::errors::StorageProviderResult;
use crate::graphql::client::static_types::NextEntryArgumentsType;

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
impl StorageProvider<StorageEntry, StorageLog, VerifiedOperation> for SqlStorage {
    type EntryArgsResponse = NextEntryArgumentsType;
    type EntryArgsRequest = EntryArgsRequest;
    type PublishEntryResponse = NextEntryArgumentsType;
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
    ) -> StorageProviderResult<Option<SchemaId>> {
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
        .bind(view_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Unwrap because we expect no invalid schema ids in the db.
        Ok(result.map(|id_str| id_str.parse().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::str::FromStr;

    use p2panda_rs::document::{DocumentView, DocumentViewFields, DocumentViewId};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{AsOperation, OperationId};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;
    use p2panda_rs::test_utils::fixtures::random_document_view_id;
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::db::traits::DocumentStore;

    /// Inserts a `DocumentView` into the db and returns its view id.
    async fn insert_document_view(db: &TestDatabase) -> DocumentViewId {
        let author = Author::try_from(db.test_data.key_pairs[0].public_key().to_owned()).unwrap();
        let entry = db
            .store
            .get_entry_at_seq_num(&author, &LogId::new(1), &SeqNum::new(1).unwrap())
            .await
            .unwrap()
            .unwrap();
        let operation_id: OperationId = entry.hash().into();
        let document_view_id: DocumentViewId = operation_id.clone().into();
        let document_view = DocumentView::new(
            &document_view_id,
            &DocumentViewFields::new_from_operation_fields(
                &operation_id,
                &entry.operation().fields().unwrap(),
            ),
        );
        let result = db
            .store
            .insert_document_view(&document_view, &SchemaId::from_str(TEST_SCHEMA_ID).unwrap())
            .await;

        assert!(result.is_ok());
        document_view_id
    }

    #[rstest]
    fn test_get_schema_for_view(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_view_id = insert_document_view(&db).await;
            let result = db
                .store
                .get_schema_by_document_view(&document_view_id)
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().unwrap().name(), "venue");
        });
    }

    #[rstest]
    fn test_get_schema_for_missing_view(
        random_document_view_id: DocumentViewId,
        #[from(test_db)]
        #[with(1, 1)]
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
