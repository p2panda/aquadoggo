// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::btree_map::Iter;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::operation::OperationValue;
use sqlx::{query, query_as};

use p2panda_rs::document::{DocumentView, DocumentViewId};
use p2panda_rs::schema::SchemaId;

use crate::db::db_types::OperationFieldRow;
use crate::db::errors::DocumentViewStorageError;
use crate::db::sql_store::SqlStorage;
use crate::db::traits::{
    AsStorageDocumentView, DocumentStore, DocumentViewFields, FieldIds, FieldName,
};
use crate::db::utils::parse_operation_fields;

/// Aquadoggo struct which will implements AsStorageDocumentView trait.
#[derive(Debug, Clone)]
pub struct DoggoDocumentView {
    document_view: DocumentView,
    field_ids: FieldIds,
    schema_id: SchemaId,
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

/// WIP: Aquadoggo implementation of the above trait
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

#[async_trait]
impl DocumentStore<DoggoDocumentView> for SqlStorage {
    /// Insert a document_view into the db. Requires that all relevent operations
    /// are already in the db as this method only creates relations between
    /// document view fields and their current values (last updated operation value).
    ///
    /// QUESTION: Is this too implementation specific? It assumes quite a lot about the db
    /// structure and others may wish to structure things differently.
    async fn insert_document_view(
        &self,
        document_view_id: &DocumentViewId,
        field_ids: &FieldIds,
        schema_id: &SchemaId,
    ) -> Result<bool, DocumentViewStorageError> {
        // OBSERVATIONS:
        // - we need to know which operation was LWW for each field.
        // - this is different from knowing the document view id, which is
        // just the tip(s) of the graph, and will likely not contain a value
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

        // Insert document view field relations into the db
        let field_relations_inserted = try_join_all(field_ids.iter().map(|field| {
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
            .bind(document_view_id.as_str())
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
        .bind(document_view_id.as_str())
        .bind(schema_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| DocumentViewStorageError::Custom(e.to_string()))?
        .rows_affected()
            == 1;
        Ok(field_relations_inserted && document_view_inserted)
    }

    /// Get a document view from the db by it'd id.
    ///
    /// Currently returns a map of document view fields as FieldName -> OperationValue.
    /// This can be specified more shortly.
    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<DocumentViewFields, DocumentViewStorageError> {
        let document_view_field_rows = query_as::<_, OperationFieldRow>(
            "
                    SELECT
                        document_view_fields.name,
                        operation_fields_v1.operation_id,
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
                        document_view_id = $1
                ",
        )
        .bind(id.hash().as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentViewStorageError::Custom(e.to_string()))?;

        Ok(parse_operation_fields(document_view_field_rows))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{
        AsOperation, Operation, OperationFields, OperationId, OperationValue,
    };
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, TEST_SCHEMA_ID};

    use crate::db::sql_store::SqlStorage;
    use crate::db::store::operation::DoggoOperation;
    use crate::db::store::test_utils::test_operation;
    use crate::db::traits::OperationStore;
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

        let mut field_ids = FieldIds::new();

        operation.fields().unwrap().keys().iter().for_each(|key| {
            field_ids.insert(key.clone(), operation_id.clone());
        });

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

        // Fake id for our first operation.
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());

        // Coresponding document id.
        let document_id = DocumentId::new(operation_id.clone());

        // The test CREATE operation which contains all fields.
        let operation = test_operation();

        // The document view now is just this one operation.
        let document_view_id: DocumentViewId = operation_id.clone().into();

        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        // Init the field ids.
        let mut field_ids = FieldIds::new();

        // Right now every field is derived from the CREATE operation, so they will all contain that id
        operation.fields().unwrap().keys().iter().for_each(|key| {
            field_ids.insert(key.clone(), operation_id.clone());
        });

        // Construct a doggo operation for publishing.
        let doggo_operation = DoggoOperation::new(&author, &operation, &operation_id, &document_id);

        // Insert the CREATE op.
        storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();

        // Insert the document view, passing in the field_ids created above.
        storage_provider
            .insert_document_view(&document_view_id, &field_ids, &schema_id)
            .await
            .unwrap();

        // Retrieve the document view.
        let result = storage_provider
            .get_document_view_by_id(&document_view_id)
            .await;

        println!("{:#?}", result);

        // Construct an UPDATE operation which only updates one field.
        let mut fields = OperationFields::new();
        fields
            .add("username", OperationValue::Text("yahoooo".to_owned()))
            .unwrap();
        let update_operation = Operation::new_update(
            SchemaId::from_str(TEST_SCHEMA_ID).unwrap(),
            vec![operation_id],
            fields,
        )
        .unwrap();

        // Give it a dummy id.
        let update_operation_id = OperationId::new(
            "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .parse()
                .unwrap(),
        );

        // Update the field_ids to include the newly update operation_id for the "username" field.
        field_ids.insert("username".to_string(), update_operation_id.clone());
        let doggo_update_operation = DoggoOperation::new(
            &author,
            &update_operation,
            &update_operation_id,
            &document_id,
        );

        // Insert the operation.
        storage_provider
            .insert_operation(&doggo_update_operation)
            .await
            .unwrap();

        // Update the document view.
        storage_provider
            .insert_document_view(&update_operation_id.clone().into(), &field_ids, &schema_id)
            .await
            .unwrap();

        // Query the new document view.
        //
        // It will combine the origin fields with the newly updated "username" field and return the completed fields.
        let result = storage_provider
            .get_document_view_by_id(&update_operation_id.into())
            .await;

        println!("{:#?}", result)
    }
}
