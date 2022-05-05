// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use futures::future::try_join_all;
use sqlx::{query, query_as, query_scalar};

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, Operation, OperationAction, OperationFields, OperationId, OperationValue,
    PinnedRelation, PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::SchemaId;

use crate::db::db_types::{OperationFieldRow, OperationRow, PreviousOperationRelationRow};
use crate::db::errors::OperationStorageError;
use crate::db::sql_store::SqlStorage;
use crate::db::traits::{AsStorageOperation, OperationStore, PreviousOperations};
use crate::db::utils::parse_operation_fields;

#[derive(Debug, Clone)]
pub struct DoggoOperation {
    author: Author,
    operation: Operation,
    id: OperationId,
    document_id: DocumentId,
}

impl DoggoOperation {
    pub fn new(
        author: &Author,
        operation: &Operation,
        operation_id: &OperationId,
        document_id: &DocumentId,
    ) -> Self {
        Self {
            author: author.clone(),
            operation: operation.clone(),
            id: operation_id.clone(),
            document_id: document_id.clone(),
        }
    }
}

impl AsStorageOperation for DoggoOperation {
    type AsStorageOperationError = OperationStorageError;

    fn action(&self) -> OperationAction {
        self.operation.action()
    }

    fn author(&self) -> Author {
        self.author.clone()
    }

    fn id(&self) -> OperationId {
        self.id.clone()
    }

    fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    fn schema_id(&self) -> SchemaId {
        self.operation.schema()
    }

    fn fields(&self) -> Option<OperationFields> {
        self.operation.fields()
    }

    fn previous_operations(&self) -> PreviousOperations {
        self.operation.previous_operations().unwrap_or_default()
    }
}

#[async_trait]
impl OperationStore<DoggoOperation> for SqlStorage {
    /// Retrieve the id of the document an operation is contained within.
    ///
    /// If no document was found, then this method returns a result wrapping
    /// a None variant.
    async fn get_document_by_operation_id(
        &self,
        id: OperationId,
    ) -> Result<Option<DocumentId>, OperationStorageError> {
        let document_id: Option<String> = query_scalar(
            "
            SELECT
                document_id
            FROM
                operations_v1
            WHERE
                operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        Ok(document_id.map(|id_str| id_str.parse().unwrap()))
    }

    /// Insert an operation into the db.
    ///
    /// This requires a DoggoEntry to be composed elsewhere, it contains an Author,
    /// DocumentId, OperationId and the actual Operation we want to store.
    ///
    /// - TODO: similar to several other places in StorageProvider, here we return
    ///   a bool wrapped in a result. We need to be more clear on when we expect to
    ///   recieve eg. an Ok(false) result, and if this is even needed.
    async fn insert_operation(
        &self,
        operation: &DoggoOperation,
    ) -> Result<bool, OperationStorageError> {
        let mut prev_op_string = "".to_string();
        for (i, operation_id) in operation.previous_operations().iter().enumerate() {
            let separator = if i == 0 { "" } else { "_" };
            prev_op_string += format!("{}{}", separator, operation_id.as_hash().as_str()).as_str();
        }

        let document_id = if operation.action().as_str() == "create" {
            DocumentId::new(operation.id())
        } else {
            // Unwrap as we know any "UPDATE" or "DELETE" operation should have previous operations
            let previous_operation_id = operation.previous_operations().get(0).unwrap().clone();

            self.get_document_by_operation_id(previous_operation_id)
                .await?
                .ok_or_else(|| OperationStorageError::Custom("Document missing".to_string()))?
        };

        // Consruct query for inserting operation row, execute it
        // and check exactly one row was affected.
        let operation_inserted = query(
            "
            INSERT INTO
                operations_v1 (
                    author,
                    document_id,
                    operation_id,
                    entry_hash,
                    action,
                    schema_id,
                    previous_operations
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(operation.author().as_str())
        .bind(document_id.as_str())
        .bind(operation.id().as_str())
        .bind(operation.id().as_hash().as_str())
        .bind(operation.action().as_str())
        .bind(operation.schema_id().as_str())
        .bind(prev_op_string.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?
        .rows_affected()
            == 1;

        // Loop over all previous operations and insert one row for
        // each, construct and execute the queries, return their futures
        // and execute all of them with `try_join_all()`.
        let previous_operations_inserted =
            try_join_all(operation.previous_operations().iter().map(|prev_op_id| {
                query(
                    "
                    INSERT INTO
                        previous_operations_v1 (
                            parent_operation_id,
                            child_operation_id
                        )
                    VALUES
                        ($1, $2)
                    ",
                )
                .bind(prev_op_id.as_str())
                .bind(operation.id().as_str().to_owned())
                .execute(&self.pool)
            }))
            .await
            // If any of the queries error we will catch that here.
            .map_err(|e| OperationStorageError::Custom(e.to_string()))?
            .iter()
            // Here we check that each query inserted exactly one row.
            .try_for_each(|result| {
                if result.rows_affected() == 1 {
                    Ok(())
                } else {
                    Err(OperationStorageError::Custom(format!(
                        "Incorrect rows affected: {}",
                        result.rows_affected()
                    )))
                }
            })
            .is_ok();

        // Same pattern as above, construct and execute the queries, return their futures
        // and execute all of them with `try_join_all()`.
        let mut fields_inserted = true;
        if let Some(fields) = operation.fields() {
            fields_inserted = try_join_all(fields.iter().flat_map(|(name, value)| {
                // Extract the field type.
                let field_type = value.field_type();

                // If the value is a relation_list or pinned_relation_list we need to insert a new field row for
                // every item in the list. Here we collect these items and return them in a vector. If this operation
                // value is anything except for the above list types, we will return a vec containing a single item.

                let db_values = match value {
                    OperationValue::Boolean(bool) => vec![Some(bool.to_string())],
                    OperationValue::Integer(int) => vec![Some(int.to_string())],
                    OperationValue::Float(float) => vec![Some(float.to_string())],
                    OperationValue::Text(str) => vec![Some(str.to_string())],
                    OperationValue::Relation(relation) => {
                        vec![Some(relation.document_id().as_str().to_string())]
                    }
                    OperationValue::RelationList(relation_list) => {
                        let mut db_values = Vec::new();
                        for document_id in relation_list.iter() {
                            db_values.push(Some(document_id.as_str().to_string()))
                        }
                        db_values
                    }
                    OperationValue::PinnedRelation(pinned_relation) => {
                        // Deriving string id here for now until implemented in p2panda-rs
                        let mut id_str = "".to_string();
                        for (i, operation_id) in
                            pinned_relation.view_id().sorted().iter().enumerate()
                        {
                            let separator = if i == 0 { "" } else { "_" };
                            id_str += format!("{}{}", separator, operation_id.as_hash().as_str())
                                .as_str();
                        }

                        vec![Some(id_str)]
                    }
                    OperationValue::PinnedRelationList(pinned_relation_list) => {
                        let mut db_values = Vec::new();
                        for document_view_id in pinned_relation_list.iter() {
                            // Deriving string id here for now until implemented in p2panda-rs
                            let mut id_str = "".to_string();
                            for (i, operation_id) in document_view_id.sorted().iter().enumerate() {
                                let separator = if i == 0 { "" } else { "_" };
                                id_str +=
                                    format!("{}{}", separator, operation_id.as_hash().as_str())
                                        .as_str();
                            }

                            db_values.push(Some(id_str))
                        }
                        db_values
                    }
                };

                // Collect all query futures.
                db_values
                    .into_iter()
                    .map(|db_value| {
                        // Compose the query and return it's future.
                        query(
                            "
                                INSERT INTO
                                    operation_fields_v1 (
                                        operation_id,
                                        name,
                                        field_type,
                                        value
                                    )
                                VALUES
                                    ($1, $2, $3, $4)
                            ",
                        )
                        .bind(operation.id().as_str().to_owned())
                        .bind(name.to_owned())
                        .bind(field_type.to_string())
                        .bind(db_value)
                        .execute(&self.pool)
                    })
                    .collect::<Vec<_>>()
            }))
            .await
            // If any queries error, we catch that here.
            .map_err(|e| OperationStorageError::Custom(e.to_string()))? // Coerce error here
            .iter()
            // All queries should perform exactly one insertion, we check that here.
            .try_for_each(|result| {
                if result.rows_affected() == 1 {
                    Ok(())
                } else {
                    Err(OperationStorageError::Custom(format!(
                        "Incorrect rows affected: {}",
                        result.rows_affected()
                    )))
                }
            })
            .is_ok();
        };
        Ok(operation_inserted && previous_operations_inserted && fields_inserted)
    }

    /// Get an operation identified by it's OperationId.
    ///
    /// Returns a DoggoOperation which includes Author, DocumentId and OperationId metadata.
    async fn get_operation_by_id(
        &self,
        id: OperationId,
    ) -> Result<Option<DoggoOperation>, OperationStorageError> {
        let operation_row = query_as::<_, OperationRow>(
            "
            SELECT
                author,
                document_id,
                operation_id,
                entry_hash,
                action,
                schema_id,
                previous_operations
            FROM
                operations_v1
            WHERE
                operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?
        .unwrap();

        let operation_fields = self.get_operation_fields_by_id(id).await?;
        let schema: SchemaId = operation_row.schema_id.parse().unwrap();

        // Need some more tooling around prev_ops in p2panda-rs
        let mut previous_operations: Vec<OperationId> = Vec::new();
        if operation_row.action != "create" {
            previous_operations = operation_row
                .previous_operations
                .rsplit('_')
                .map(|id_str| Hash::new(id_str).unwrap().into())
                .collect();
        }

        let operation = match operation_row.action.as_str() {
            "create" => Operation::new_create(schema, operation_fields),
            "update" => Operation::new_update(schema, previous_operations, operation_fields),
            "delete" => Operation::new_delete(schema, previous_operations),
            _ => panic!(),
        }
        .unwrap();

        Ok(Some(DoggoOperation::new(
            &Author::new(&operation_row.author).unwrap(),
            &operation,
            &operation_row.operation_id.parse().unwrap(),
            &operation_row.document_id.parse().unwrap(),
        )))
    }

    /// Get just the fields of an operation, identified by it's OperationId.
    async fn get_operation_fields_by_id(
        &self,
        id: OperationId,
    ) -> Result<OperationFields, OperationStorageError> {
        let operation_field_rows = query_as::<_, OperationFieldRow>(
            "
            SELECT
                operation_id,
                name,
                field_type,
                value
            FROM
                operation_fields_v1
            WHERE
                operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        Ok(parse_operation_fields(operation_field_rows))
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::operation::OperationId;
    use p2panda_rs::test_utils::constants::DEFAULT_HASH;
    use p2panda_rs::{document::DocumentId, identity::Author};

    use crate::db::sql_store::SqlStorage;
    use crate::db::store::test_utils::test_operation;
    use crate::db::traits::AsStorageOperation;
    use crate::test_helpers::initialize_db;

    use super::{DoggoOperation, OperationStore};

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[tokio::test]
    async fn insert_operation() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        // Create Author, OperationId and DocumentId in order to compose a DoggoOperation.
        let author = Author::new(TEST_AUTHOR).unwrap();
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let doggo_operation =
            DoggoOperation::new(&author, &test_operation(), &operation_id, &document_id);

        // Insert the doggo operation into the db, returns Ok(true) when succesful.
        let result = storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();

        assert!(result);

        // Request the previously inserted operation by it's id.
        let returned_doggo_operation = storage_provider
            .get_operation_by_id(operation_id.clone())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(returned_doggo_operation.author(), doggo_operation.author());
        assert_eq!(returned_doggo_operation.fields(), doggo_operation.fields());
        assert_eq!(returned_doggo_operation.id(), doggo_operation.id());
        assert_eq!(
            returned_doggo_operation.document_id(),
            doggo_operation.document_id()
        );
    }

    async fn get_operation_fields() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        // Create Author, OperationId and DocumentId in order to compose a DoggoOperation.
        let author = Author::new(TEST_AUTHOR).unwrap();
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let doggo_operation =
            DoggoOperation::new(&author, &test_operation(), &operation_id, &document_id);

        // Insert the doggo operation into the db, returns Ok(true) when succesful.
        let result = storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();

        assert!(result);

        // Get the operation fields for an operation identified by it's OperationId.
        let result = storage_provider
            .get_operation_fields_by_id(operation_id.clone())
            .await
            .unwrap();

        assert_eq!(result, doggo_operation.fields().unwrap());
    }
}
