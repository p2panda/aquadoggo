// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::OperationStorageError;
use sqlx::query_scalar;

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
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_view_id};
    use p2panda_rs::{document::DocumentViewId, schema::FieldType};
    use rstest::rstest;

    use crate::db::stores::test_utils::{add_schema, test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    fn test_get_schema_for_view(
        key_pair: KeyPair,
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|mut db: TestDatabase| async move {
            let schema = add_schema(
                &mut db,
                "venue",
                vec![
                    ("description", FieldType::String),
                    ("profile_name", FieldType::String),
                ],
                &key_pair,
            )
            .await;

            let document_view_id = match schema.id() {
                SchemaId::Application(_, view_id) => view_id,
                _ => panic!("Invalid schema id"),
            };

            let result = db.store.get_schema_by_document_view(document_view_id).await;

            assert!(result.is_ok());
            // This is the schema name of the schema document we published.
            assert_eq!(result.unwrap().unwrap().name(), "schema_definition");
        });
    }

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
