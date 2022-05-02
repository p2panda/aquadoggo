// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::schema::SchemaId;
use sqlx::any::AnyRow;
use sqlx::{query, query_as, FromRow};

use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::operation::{
    OperationFields, OperationId, OperationValue, PinnedRelation, PinnedRelationList, Relation,
    RelationList,
};
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

use super::operation::OperationStore;

type FieldName = String;
type DocumentViewFields = BTreeMap<FieldName, OperationValue>;

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
    name: String,

    field_type: String,

    value: String,
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

    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<DocumentViewFields, DocumentViewStorageError>;
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

    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<DocumentViewFields, DocumentViewStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
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
                document_view_id_hash = $1
            ",
        )
        .bind(id.hash().as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentViewStorageError::Custom(e.to_string()))?;

        let mut relation_list: Vec<DocumentId> = Vec::new();
        let mut pinned_relation_list: Vec<DocumentViewId> = Vec::new();

        let mut document_view_fields = DocumentViewFields::new();

        // Iterate over returned field values, for each value:
        //  - if it is a simple value type, parse it into an OperationValue and add it to the operation_fields
        //  - if it is a relation list value type parse each item into a DocumentId/DocumentViewId and push to
        //    the suitable vec (instantiated above)
        document_view_field_rows.iter().for_each(|row| {
            match row.field_type.as_str() {
                "bool" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::Boolean(row.value.parse::<bool>().unwrap()),
                    );
                }
                "int" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::Integer(row.value.parse::<i64>().unwrap()),
                    );
                }
                "float" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::Float(row.value.parse::<f64>().unwrap()),
                    );
                }
                "str" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::Text(row.value.clone()),
                    );
                }
                "relation" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::Relation(Relation::new(
                            row.value.parse::<DocumentId>().unwrap(),
                        )),
                    );
                }
                // A special case, this is a list item, so we push it to a vec but _don't_ add it
                // to the document_view_fields yet.
                "relation_list" => relation_list.push(row.value.parse::<DocumentId>().unwrap()),
                "pinned_relation" => {
                    document_view_fields.insert(
                        row.name.to_string(),
                        OperationValue::PinnedRelation(PinnedRelation::new(
                            row.value.parse::<DocumentViewId>().unwrap(),
                        )),
                    );
                }
                // A special case, this is a list item, so we push it to a vec but _don't_ add it
                // to the document_view_fields yet.
                "pinned_relation_list" => {
                    pinned_relation_list.push(row.value.parse::<DocumentViewId>().unwrap())
                }
                _ => (),
            };
        });

        // Find if there is at least one field containing a "relation_list" type
        let relation_list_field = &document_view_field_rows
            .iter()
            .find(|row| row.field_type == "relation_list");

        // If so, then parse the `relation_list` vec into an operation value and add it to the document view fields
        if let Some(relation_list_field) = relation_list_field {
            document_view_fields.insert(
                relation_list_field.name.to_string(),
                OperationValue::RelationList(RelationList::new(relation_list)),
            );
        }

        // Find if there is at least one field containing a "pinned_relation_list" type
        let pinned_relation_list_field = &document_view_field_rows
            .iter()
            .find(|row| row.field_type == "pinned_relation_list");

        // If so, then parse the `pinned_relation_list` vec into an operation value and add it to the document view fields
        if let Some(pinned_relation_list_field) = pinned_relation_list_field {
            document_view_fields.insert(
                pinned_relation_list_field.name.to_string(),
                OperationValue::PinnedRelationList(PinnedRelationList::new(pinned_relation_list)),
            );
        }

        Ok(document_view_fields)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{AsOperation, OperationId};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, TEST_SCHEMA_ID};

    use crate::db::models::operation::{DoggoOperation, OperationStore};
    use crate::db::models::test_utils::test_operation;
    use crate::db::store::SqlStorage;
    use crate::test_helpers::initialize_db;

    use super::{DocumentStore, FieldIds};

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

    #[tokio::test]
    async fn get_document_view() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };
        let author = Author::new(TEST_AUTHOR).unwrap();

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let operation = test_operation();
        let document_view_id: DocumentViewId = operation_id.clone().into();
        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        let mut field_ids = BTreeMap::new();

        operation.fields().unwrap().keys().iter().for_each(|key| {
            field_ids.insert(key.clone(), operation_id.clone());
        });
        let field_ids = FieldIds(field_ids);

        let doggo_operation = DoggoOperation::new(&operation, &operation_id, &author);

        storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();

        storage_provider
            .insert_document_view(&document_view_id, &field_ids, &schema_id)
            .await
            .unwrap();

        let result = storage_provider
            .get_document_view_by_id(&document_view_id)
            .await;

        println!("{:#?}", result)
    }
}
