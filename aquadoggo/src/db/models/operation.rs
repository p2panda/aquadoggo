// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use futures::future::try_join_all;
use serde::{Serialize, Serializer};
use sqlx::query;

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{OperationAction, OperationFields, OperationId};
use p2panda_rs::schema::{FieldType, SchemaId};
use p2panda_rs::Validate;

use crate::db::store::SqlStorage;

/////// DB ROWS ///////
// Structs representing data as it is stored in rows in the db
pub struct OperationRow {
    id: String,
    author: String,
    action: String,
    entry_hash: String,
    schema_id_short: String,
}

pub struct PreviousOperationRelationRow {
    parent_operation_id: String,
    child_operation_id: String,
}

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

    fn fields(&self) -> OperationFields;

    fn id(&self) -> OperationId;

    fn previous_operations(&self) -> Vec<OperationId>;

    fn schema_id(&self) -> SchemaId;

    fn schema_id_short(&self) -> SchemaIdShort;
}

/// `LogStorage` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Ahhhhh!!!!")]
    Error,
}

#[async_trait]
pub trait OperationStore<StorageOperation: AsStorageOperation> {
    async fn insert_operation(
        &self,
        operation: &StorageOperation,
    ) -> Result<bool, OperationStorageError>;

    // async fn get_operation(
    //     &self,
    //     id: OperationId,
    // ) -> Result<StorageOperation, OperationStorageError>;
}

////// IMPLEMENT THE STORAGE TRAITS //////

#[derive(Debug, Clone)]
pub struct DoggoOperation {
    action: OperationAction,
    author: Author,
    document_id: DocumentId,
    document_view_id_hash: DocumentViewIdHash,
    fields: OperationFields,
    id: OperationId,
    previous_operations: Vec<OperationId>,
    schema_id: SchemaId,
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

    fn fields(&self) -> OperationFields {
        self.fields.clone()
    }

    fn previous_operations(&self) -> Vec<OperationId> {
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
                    schema_id_short,
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
        .map_err(|e| OperationStorageError::Error)?
        .rows_affected()
            == 1;

        let previous_operations_inserted =
            try_join_all(operation.previous_operations().iter().map(|prev_op_id| {
                query(
                    "
            INSERT INTO
                previous_operations_v1 (
                    parent_operation_id,
                    child_operation_id,
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
            .map_err(|e| OperationStorageError::Error)? // Coerce error here
            .iter()
            .try_for_each(|result| {
                if result.rows_affected() == 1 {
                    Ok(())
                } else {
                    Err(OperationStorageError::Error)
                }
            })
            .is_ok();

        let fields_inserted = try_join_all(operation.fields().iter().map(|field| {
            query(
                "
                INSERT INTO
                    operation_fields_v1 (
                        operation_id,
                        name,
                        field_type,
                        value,
                        relation_document_id,
                        relation_document_view_id_hash,
                    )
                VALUES
                    ($1, $2, $3, $4, $5)
            ",
            )
            .bind(operation.id().as_str().to_owned())
            .bind(field.0.to_owned())
            .bind(field.1.field_type())
            .bind(operation.document_id().as_str().to_owned())
            .bind(operation.document_view_id_hash().as_str().to_owned())
            .execute(&self.pool)
        }))
        .await
        .map_err(|e| OperationStorageError::Error)? // Coerce error here
        .iter()
        .try_for_each(|result| {
            if result.rows_affected() == 1 {
                Ok(())
            } else {
                Err(OperationStorageError::Error)
            }
        })
        .is_ok();

        Ok(operation_inserted && previous_operations_inserted && fields_inserted)
    }
}
