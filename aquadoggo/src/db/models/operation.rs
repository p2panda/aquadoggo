// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use futures::future::try_join_all;
use sqlx::{query, query_as, FromRow};

use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, Operation, OperationAction, OperationFields, OperationId, OperationValue,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

/// A struct representing a single operation row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationRow {
    /// The id of this operation.
    operation_id: String,

    /// The author of this operation.
    author: String,

    /// The action type this operation is performing.
    action: String,

    /// The hash of the entry this operation is associated with.
    entry_hash: String,

    /// The id of the schema this operation follows.
    schema_id_short: String,
}

/// A struct representing a single previous operation relation row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct PreviousOperationRelationRow {
    /// The parent in this operation relation. This is the operation
    /// being appended to, it lies nearer the root in a graph structure.
    parent_operation_id: String,

    /// The child in this operation relation. This is the operation
    /// which has a depenency on the parent, it lies nearer the tip/leaves
    /// in a graph structure.
    child_operation_id: String,
}

/// A struct representing a single operation field row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationFieldRow {
    /// The id of the operation this field was published on.
    operation_id: String,

    /// The name of this field.
    name: String,

    /// The type of this field.
    field_type: String,

    /// The actual value contained in this field.
    value: String,
}

type PreviousOperations = Vec<OperationId>;
type SchemaIdShort = String;

pub trait AsStorageOperation: Sized + Clone + Send + Sync + Validate {
    /// The error type returned by this traits' methods.
    type AsStorageOperationError: 'static + std::error::Error;

    fn action(&self) -> OperationAction;

    fn author(&self) -> Author;

    fn fields(&self) -> Option<OperationFields>;

    fn id(&self) -> OperationId;

    fn previous_operations(&self) -> PreviousOperations;

    fn schema_id(&self) -> SchemaId;

    fn schema_id_short(&self) -> SchemaIdShort;
}

/// `OperationStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Ahhhhh!!!!: {0}")]
    Custom(String),
}

#[async_trait]
pub trait OperationStore<StorageOperation: AsStorageOperation> {
    async fn insert_operation(
        &self,
        operation: &StorageOperation,
    ) -> Result<bool, OperationStorageError>;

    async fn get_operation_by_id(
        &self,
        id: OperationId,
    ) -> Result<
        (
            Option<OperationRow>,
            Vec<PreviousOperationRelationRow>,
            Vec<OperationFieldRow>,
        ),
        OperationStorageError,
    >;
}

#[derive(Debug, Clone)]
pub struct DoggoOperation {
    author: Author,
    id: OperationId,
    operation: Operation,
}

impl DoggoOperation {
    pub fn new(operation: &Operation, operation_id: &OperationId, author: &Author) -> Self {
        Self {
            author: author.clone(),
            id: operation_id.clone(),
            operation: operation.clone(),
        }
    }
}

impl Validate for DoggoOperation {
    type Error = OperationStorageError;

    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
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

    fn schema_id_short(&self) -> SchemaIdShort {
        match &self.operation.schema() {
            SchemaId::Application(name, document_view_id) => {
                format!("{}__{}", name, document_view_id.hash().as_str())
            }
            _ => self.operation.schema().as_str(),
        }
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
    async fn insert_operation(
        &self,
        operation: &DoggoOperation,
    ) -> Result<bool, OperationStorageError> {
        // Convert the action to a string.
        let action = match operation.action() {
            OperationAction::Create => "create",
            OperationAction::Update => "update",
            OperationAction::Delete => "delete",
        };

        // Consruct query for inserting operation row, execute it
        // and check exactly one row was affected.
        let operation_inserted = query(
            "
            INSERT INTO
                operations_v1 (
                    author,
                    operation_id,
                    entry_hash,
                    action,
                    schema_id_short
                )
            VALUES
                ($1, $2, $3, $4, $5)
            ",
        )
        .bind(operation.author().as_str())
        .bind(operation.id().as_str())
        .bind(operation.id().as_hash().as_str())
        .bind(action)
        .bind(operation.schema_id_short().as_str())
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
                        vec![Some(pinned_relation.view_id().hash().as_str().to_string())]
                    }
                    OperationValue::PinnedRelationList(pinned_relation_list) => {
                        let mut db_values = Vec::new();
                        for document_view_id in pinned_relation_list.iter() {
                            // I think we'd prefer to store the full id here, not the hash, don't think there's
                            // a string method for that yet though.
                            db_values.push(Some(document_view_id.hash().as_str().to_string()))
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

    // Which getters do we actually want here? Right now, if we want to retrieve a complete operation, it's easier to do it
    // via `EntryStore` by getting the encoded operation. We may want more atomic getters here for field_type, value, action etc..
    async fn get_operation_by_id(
        &self,
        id: OperationId,
    ) -> Result<
        (
            Option<OperationRow>,
            Vec<PreviousOperationRelationRow>,
            Vec<OperationFieldRow>,
        ),
        OperationStorageError,
    > {
        let operation_row = query_as::<_, OperationRow>(
            "
            SELECT
                author,
                operation_id,
                entry_hash,
                action,
                schema_id_short
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

        let previous_operation_rows = query_as::<_, PreviousOperationRelationRow>(
            "
            SELECT
                parent_operation_id,
                child_operation_id
            FROM
                previous_operations_v1
            WHERE
                child_operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

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

        Ok((operation_row, previous_operation_rows, operation_field_rows))
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::OperationId;
    use p2panda_rs::test_utils::constants::DEFAULT_HASH;

    use crate::db::models::test_utils::test_operation;
    use crate::db::store::SqlStorage;
    use crate::test_helpers::initialize_db;

    use super::{DoggoOperation, OperationStore};

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[tokio::test]
    async fn insert_operation() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());

        let doggo_operation = DoggoOperation::new(&test_operation(), &operation_id, &author);

        let result = storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();
        assert!(result);

        let result = storage_provider
            .get_operation_by_id(operation_id.clone())
            .await
            .unwrap();

        let (operation_row, previous_operation_relation_rows, field_rows) = result;

        assert!(operation_row.is_some());
        assert_eq!(previous_operation_relation_rows.len(), 0);
        assert_eq!(field_rows.len(), 10);

        println!(
            "{:#?}",
            (operation_row, previous_operation_relation_rows, field_rows)
        );
    }
}
