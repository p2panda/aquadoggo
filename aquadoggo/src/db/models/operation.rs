// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use futures::future::try_join_all;
use serde::{Serialize, Serializer};
use sqlx::{query, query_as, FromRow};

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, Operation, OperationAction, OperationEncoded, OperationFields, OperationId,
};
use p2panda_rs::schema::{FieldType, SchemaId};
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

/////// DB ROWS ///////
// Structs representing data as it is stored in rows in the db
#[derive(FromRow, Debug)]
pub struct OperationRow {
    operation_id: String,
    author: String,
    action: String,
    entry_hash: String,
    schema_id_short: String,
}

#[derive(FromRow, Debug)]
pub struct PreviousOperationRelationRow {
    parent_operation_id: String,
    child_operation_id: String,
}

#[derive(FromRow, Debug)]
pub struct OperationFieldRow {
    operation_id: String,
    name: String,
    field_type: String,
    value: String,
    relation_document_id: String,
    relation_document_view_id_hash: String,
}

type DocumentViewIdHash = Hash;
type SchemaIdShort = String;

////// OPERATION STORAGE TRAITS ///////

pub trait AsStorageOperation: Sized + Clone + Send + Sync + Validate {
    /// The error type returned by this traits' methods.
    type AsStorageOperationError: 'static + std::error::Error;

    fn action(&self) -> OperationAction;

    fn author(&self) -> Author;

    fn document_id(&self) -> DocumentId;

    // // and the operation id graph tips in order to reconstruct the
    // // complete DocumentViewId if it's needed (is it needed?).
    // // We could store relations between document view id hashes
    // fn document_view_id(&self) -> DocumentViewId;

    fn document_view_id_hash(&self) -> DocumentViewIdHash;

    fn fields(&self) -> Option<OperationFields>;

    fn id(&self) -> OperationId;

    fn previous_operations(&self) -> PreviousOperations;

    fn schema_id(&self) -> SchemaId;

    fn schema_id_short(&self) -> SchemaIdShort;
}

/// `LogStorage` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Ahhhhh!!!!: {0}")]
    Custom(String),
}

type PreviousOperations = Vec<OperationId>;

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

////// IMPLEMENT THE STORAGE TRAITS //////

#[derive(Debug, Clone)]
pub struct DoggoOperation {
    // We don't need all these fields, we could just store the encoded operation, author, and document_id for example
    action: OperationAction,
    author: Author,
    document_id: DocumentId,
    document_view_id_hash: DocumentViewIdHash,
    fields: Option<OperationFields>,
    id: OperationId,
    previous_operations: PreviousOperations,
    schema_id: SchemaId,
}

impl DoggoOperation {
    pub fn new(
        operation_encoded: &OperationEncoded,
        document_id: &DocumentId,
        author: &Author,
    ) -> Self {
        let decoded_operation = Operation::from(operation_encoded);
        let document_view_id = DocumentViewId::from(operation_encoded.hash());
        let operation_id = OperationId::from(operation_encoded.hash());
        Self {
            action: decoded_operation.action(),
            author: author.clone(),
            document_id: document_id.clone(),
            document_view_id_hash: document_view_id.hash(),
            fields: decoded_operation.fields(),
            id: operation_id,
            previous_operations: decoded_operation.previous_operations().unwrap_or_default(),
            schema_id: decoded_operation.schema(),
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
        self.action.clone()
    }

    fn author(&self) -> Author {
        self.author.clone()
    }

    fn id(&self) -> OperationId {
        self.id.clone()
    }

    fn schema_id_short(&self) -> SchemaIdShort {
        match &self.schema_id {
            SchemaId::Application(name, document_view_id) => {
                format!("{}__{}", name, document_view_id.hash().as_str())
            }
            _ => self.schema_id.as_str(),
        }
    }

    fn schema_id(&self) -> SchemaId {
        self.schema_id.clone()
    }

    fn fields(&self) -> Option<OperationFields> {
        self.fields.clone()
    }

    fn previous_operations(&self) -> PreviousOperations {
        self.previous_operations.clone()
    }

    fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    fn document_view_id_hash(&self) -> DocumentViewIdHash {
        self.document_view_id_hash.clone()
    }
}

#[async_trait]
impl OperationStore<DoggoOperation> for SqlStorage {
    async fn insert_operation(
        &self,
        operation: &DoggoOperation,
    ) -> Result<bool, OperationStorageError> {
        // We need `as_str()` for OperationAction
        let action = match operation.action() {
            OperationAction::Create => "create",
            OperationAction::Update => "update",
            OperationAction::Delete => "delete",
        };
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
            .map_err(|e| OperationStorageError::Custom(e.to_string()))? // Coerce error here
            .iter()
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

        let mut fields_inserted = true;
        if let Some(fields) = operation.fields() {
            fields_inserted = try_join_all(fields.iter().map(|(name, value)| {
                query(
                    "
                    INSERT INTO
                        operation_fields_v1 (
                            operation_id,
                            name,
                            field_type,
                            value,
                            relation_document_id,
                            relation_document_view_id_hash
                        )
                    VALUES
                        ($1, $2, $3, $4, $5, $6)
                ",
                )
                .bind(operation.id().as_str().to_owned())
                .bind(name.to_owned())
                .bind(value.field_type())
                // This needs some thought..... how to store the value as a string?
                .bind("Yelp!")
                .bind(operation.document_id().as_str().to_owned())
                .bind(operation.document_view_id_hash().as_str().to_owned())
                .execute(&self.pool)
            }))
            .await
            .map_err(|e| OperationStorageError::Custom(e.to_string()))? // Coerce error here
            .iter()
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
                value,
                relation_document_id,
                relation_document_view_id_hash
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
    use std::convert::TryFrom;
    use std::str::FromStr;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, TEST_SCHEMA_ID};

    use crate::db::store::SqlStorage;
    use crate::test_helpers::initialize_db;

    use super::{DoggoOperation, OperationStore};

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[tokio::test]
    async fn insert_operation() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();

        let mut fields = OperationFields::new();
        fields
            .add(
                "venue_name",
                OperationValue::Text("Shirokuma Cafe".to_string()),
            )
            .unwrap();
        let operation =
            Operation::new_create(SchemaId::from_str(TEST_SCHEMA_ID).unwrap(), fields).unwrap();

        let document_id = DocumentId::new(DEFAULT_HASH.parse().unwrap());

        let doggo_operation = DoggoOperation::new(
            &OperationEncoded::try_from(&operation).unwrap(),
            &document_id,
            &author,
        );
        let result = storage_provider
            .insert_operation(&doggo_operation)
            .await
            .unwrap();
        assert!(result);

        let result = storage_provider
            .get_operation_by_id(
                OperationEncoded::try_from(&operation)
                    .unwrap()
                    .hash()
                    .into(),
            )
            .await
            .unwrap();

        println!("{:#?}", result);
    }
}
