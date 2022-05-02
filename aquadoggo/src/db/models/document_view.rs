// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::schema::SchemaId;
use sqlx::{query, FromRow};

use p2panda_rs::document::{DocumentView, DocumentViewId};
use p2panda_rs::operation::{OperationId, OperationValue};
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

type FieldName = String;

// We can derive this quite simply from a sorted list of operations by visiting
// each operation from the end of the list until we have found the id for every
// field in this view/schema.
#[derive(FromRow, Debug, Clone)]
pub struct FieldIds(BTreeMap<FieldName, OperationId>);

#[derive(FromRow, Debug)]
pub struct DocumentViewRow {
    document_view_id_hash: String,

    schema_id_short: String,
}

#[derive(FromRow, Debug)]
pub struct DocumentViewFieldRow {
    document_view_id_hash: String,

    operation_id: String,

    name: String,
}

#[derive(Debug, Clone)]
pub struct DoggoDocumentView {
    document_view: DocumentView,
    field_ids: FieldIds,
    schema_id: SchemaId,
}

pub trait AsStorageDocumentView: Sized + Clone + Send + Sync + Validate {
    /// The error type returned by this traits' methods.
    type AsStorageDocumentViewError: 'static + std::error::Error;

    fn id(&self) -> DocumentViewId;
    fn iter(&self) -> Iter<FieldName, OperationValue>;
    fn get(&self, key: &str) -> Option<&OperationValue>;
    fn schema_id(&self) -> SchemaId;
    fn field_ids(&self) -> FieldIds;
}

impl DoggoDocumentView {
    pub fn new(document_view: &DocumentView, field_ids: &FieldIds, schema_id: &SchemaId) -> Self {
        Self {
            document_view: document_view.clone(),
            field_ids: field_ids.clone(),
            schema_id: schema_id.clone(),
        }
    }
}

impl Validate for DoggoDocumentView {
    type Error = DocumentViewStorageError;

    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl AsStorageDocumentView for DoggoDocumentView {
    type AsStorageDocumentViewError = DocumentViewStorageError;

    fn id(&self) -> DocumentViewId {
        self.document_view.id().clone()
    }
    fn iter(&self) -> Iter<FieldName, OperationValue> {
        self.document_view.iter()
    }
    fn get(&self, key: &str) -> Option<&OperationValue> {
        self.document_view.get(key)
    }
    fn schema_id(&self) -> SchemaId {
        self.schema_id.clone()
    }
    fn field_ids(&self) -> FieldIds {
        self.field_ids.clone()
    }
}

/// `DocumentStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum DocumentViewStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Ahhhhh!!!!: {0}")]
    Custom(String),
}

#[async_trait]
pub trait DocumentStore<StorageDocumentView: AsStorageDocumentView> {
    async fn insert_document_view(
        &self,
        document_view: &DocumentViewId,
        field_ids: &FieldIds,
        schema_id: &SchemaId,
    ) -> Result<bool, DocumentViewStorageError>;
}

#[async_trait]
impl DocumentStore<DoggoDocumentView> for SqlStorage {
    async fn insert_document_view(
        &self,
        document_view_id: &DocumentViewId,
        field_ids: &FieldIds,
        schema_id: &SchemaId,
    ) -> Result<bool, DocumentViewStorageError> {
        // Observations:
        // - we need to know which operation was LWW for each field.
        // - this is different from knowing the document view id, which is
        // just the tip(s) of the graph, and will likely now contain a value
        // for every field.
        // - we could record the operation id for each value when we build the
        //   document
        // - alternatively we could do some dynamic "reverse" graph traversal
        // starting from the document view id. This would require
        // implementing some new traversal logic (maybe it is already underway
        // somewhere? I remember @cafca working on this a while ago).
        // - we could also pass in a full list of already sorted operations,
        // these are already stored on `Document` so could be re-used from
        // there.
        let field_relations_inserted = try_join_all(field_ids.0.iter().map(|field| {
            query(
                "
                    INSERT INTO
                        document_view_fields (
                            document_view_id_hash,
                            operation_id,
                            name
                        )
                    VALUES
                        ($1, $2, $3)
                    ",
            )
            .bind(document_view_id.hash().as_str().to_string())
            .bind(field.1.as_str().to_owned())
            .bind(field.0.as_str())
            .execute(&self.pool)
        }))
        .await
        .map_err(|e| DocumentViewStorageError::Custom(e.to_string()))?
        .iter()
        .try_for_each(|result| {
            if result.rows_affected() == 1 {
                Ok(())
            } else {
                Err(DocumentViewStorageError::Custom(format!(
                    "Incorrect rows affected: {}",
                    result.rows_affected()
                )))
            }
        })
        .is_ok();

        let schema_id_short = match &schema_id {
            SchemaId::Application(name, document_view_id) => {
                format!("{}__{}", name, document_view_id.hash().as_str())
            }
            _ => schema_id.as_str(),
        };

        let operation_inserted = query(
            "
            INSERT INTO
                document_views (
                    document_view_id_hash,
                    schema_id_short
                )
            VALUES
                ($1, $2)
            ",
        )
        .bind(document_view_id.hash().as_str())
        .bind(schema_id_short)
        .execute(&self.pool)
        .await
        .map_err(|e| DocumentViewStorageError::Custom(e.to_string()))?
        .rows_affected()
            == 1;
        Ok(field_relations_inserted && operation_inserted)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::operation::{AsOperation, OperationId};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, TEST_SCHEMA_ID};

    use crate::db::models::test_utils::test_operation;
    use crate::db::store::SqlStorage;
    use crate::test_helpers::initialize_db;

    use super::{DocumentStore, DoggoDocumentView, FieldIds};

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[tokio::test]
    async fn insert_document_view() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let operation = test_operation();
        let document_view_id: DocumentViewId = operation_id.clone().into();
        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        let mut field_ids = BTreeMap::new();

        operation.fields().unwrap().keys().iter().for_each(|key| {
            field_ids.insert(key.clone(), operation_id.clone());
        });
        let field_ids = FieldIds(field_ids);

        let result = storage_provider
            .insert_document_view(&document_view_id, &field_ids, &schema_id)
            .await;

        assert!(result.unwrap());
    }
}
