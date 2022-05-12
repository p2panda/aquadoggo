// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::{DocumentView, DocumentViewFields, DocumentViewId, DocumentViewValue};
use p2panda_rs::schema::SchemaId;
use sqlx::{query, query_as};

use crate::db::errors::DocumentStorageError;
use crate::db::models::document::DocumentViewFieldRow;
use crate::db::provider::SqlStorage;
use crate::db::traits::{AsStorageDocumentView, DocumentStore};
use crate::db::utils::parse_document_view_field_rows;

/// Aquadoggo struct which will implement AsStorageDocumentView trait.
#[derive(Debug, Clone)]
pub struct StorageDocumentView(DocumentView);

impl StorageDocumentView {
    pub fn new(id: &DocumentViewId, fields: &DocumentViewFields) -> Self {
        Self(DocumentView::new(id.clone(), fields.clone()))
    }
}

impl AsStorageDocumentView for StorageDocumentView {
    type AsStorageDocumentViewError = DocumentStorageError;

    fn id(&self) -> &DocumentViewId {
        self.0.id()
    }

    fn iter(&self) -> Iter<String, DocumentViewValue> {
        self.0.iter()
    }

    fn get(&self, key: &str) -> Option<&DocumentViewValue> {
        self.0.get(key)
    }

    fn fields(&self) -> &DocumentViewFields {
        self.0.fields()
    }
}

#[async_trait]
impl DocumentStore<StorageDocumentView> for SqlStorage {
    /// Insert a document_view into the db.
    async fn insert_document_view(
        &self,
        document_view: &StorageDocumentView,
        schema_id: &SchemaId,
    ) -> Result<bool, DocumentStorageError> {
        // Insert document view field relations into the db
        let field_relations_inserted = try_join_all(document_view.iter().map(|(name, value)| {
            query(
                "
                INSERT INTO
                    document_view_fields (
                        document_view_id,
                        operation_id,
                        name
                    )
                VALUES
                    ($1, $2, $3)
                ",
            )
            .bind(document_view.id().as_str())
            .bind(value.id().as_str().to_owned())
            .bind(name)
            .execute(&self.pool)
        }))
        .await
        .map_err(|e| DocumentStorageError::Custom(e.to_string()))?
        .iter()
        .try_for_each(|result| {
            if result.rows_affected() == 1 {
                Ok(())
            } else {
                Err(DocumentStorageError::Custom(format!(
                    "Incorrect rows affected: {}",
                    result.rows_affected()
                )))
            }
        })
        .is_ok();

        // Insert document view fields into the db
        let document_view_inserted = query(
            "
            INSERT INTO
                document_views (
                    document_view_id,
                    schema_id
                )
            VALUES
                ($1, $2)
            ",
        )
        .bind(document_view.id().as_str())
        .bind(schema_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::Custom(e.to_string()))?
        .rows_affected()
            == 1;

        Ok(field_relations_inserted && document_view_inserted)
    }

    /// Get a document view from the database by it's id.
    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<StorageDocumentView, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                document_view_fields.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
                operation_fields_v1.field_type,
                operation_fields_v1.value
            FROM
                document_view_fields
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = document_view_fields.operation_id
                AND
                    operation_fields_v1.name = document_view_fields.name
            WHERE
                document_view_fields.document_view_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::Custom(e.to_string()))?;

        Ok(StorageDocumentView::new(
            &id.to_owned(),
            &parse_document_view_field_rows(document_view_field_rows),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::str::FromStr;

    use p2panda_rs::document::{DocumentViewFields, DocumentViewValue};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{AsOperation, OperationId};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore};
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};

    use crate::db::stores::document::{DocumentStore, StorageDocumentView};
    use crate::db::stores::test_utils::{test_create_operation, test_db};

    #[tokio::test]
    async fn insert_document_view() {
        let storage_provider = test_db(0, false).await;

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_view = StorageDocumentView::new(
            &operation_id.clone().into(),
            &DocumentViewFields::new_from_operation_fields(
                &operation_id,
                &test_create_operation().fields().unwrap(),
            ),
        );

        let result = storage_provider
            .insert_document_view(&document_view, &SchemaId::from_str(TEST_SCHEMA_ID).unwrap())
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_document_view() {
        let storage_provider = test_db(2, false).await;
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        let log_id = LogId::default();
        let seq_num = SeqNum::default();

        let entry = storage_provider
            .entry_at_seq_num(&author, &log_id, &seq_num)
            .await
            .unwrap()
            .unwrap();

        let operation_id = entry.hash().into();
        let mut document_view_fields = DocumentViewFields::new_from_operation_fields(
            &operation_id,
            &entry.operation().fields().unwrap(),
        );

        let document_view =
            StorageDocumentView::new(&operation_id.clone().into(), &document_view_fields);

        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        // Insert the document view.
        storage_provider
            .insert_document_view(&document_view, &schema_id)
            .await
            .unwrap();

        // Retrieve the document view.
        let result = storage_provider
            .get_document_view_by_id(&operation_id.clone().into())
            .await;

        println!("{:#?}", result);
        assert!(result.is_ok());

        let seq_num = SeqNum::new(2).unwrap();

        let entry = storage_provider
            .entry_at_seq_num(&author, &log_id, &seq_num)
            .await
            .unwrap()
            .unwrap();

        let operation_id: OperationId = entry.hash().into();

        document_view_fields.insert(
            "username",
            DocumentViewValue::Value(
                operation_id.clone(),
                entry
                    .operation()
                    .fields()
                    .unwrap()
                    .get("username")
                    .unwrap()
                    .clone(),
            ),
        );

        let document_view =
            StorageDocumentView::new(&operation_id.clone().into(), &document_view_fields);

        // Update the document view.
        storage_provider
            .insert_document_view(&document_view, &schema_id)
            .await
            .unwrap();

        // Query the new document view.
        //
        // It will combine the origin fields with the newly updated "username" field and return the completed fields.
        let result = storage_provider
            .get_document_view_by_id(&operation_id.into())
            .await;

        println!("{:#?}", result);
        assert!(result.is_ok())
    }
}
