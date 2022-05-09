// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{OperationAction, OperationFields, OperationId};
use p2panda_rs::schema::SchemaId;

use crate::db::errors::OperationStorageError;

pub type PreviousOperations = Vec<OperationId>;

pub trait AsStorageOperation: Sized + Clone + Send + Sync {
    /// The error type returned by this traits' methods.
    type AsStorageOperationError: 'static + std::error::Error;

    fn action(&self) -> OperationAction;

    fn author(&self) -> Author;

    fn document_id(&self) -> DocumentId;

    fn fields(&self) -> Option<OperationFields>;

    fn id(&self) -> OperationId;

    fn previous_operations(&self) -> PreviousOperations;

    fn schema_id(&self) -> SchemaId;
}

#[async_trait]
pub trait OperationStore<StorageOperation: AsStorageOperation> {
    /// Insert an operation into the db.
    ///
    /// The passed operation must implement the `AsStorageOperation` trait. Errors when
    /// a fatal DB error occurs, returns true or false depending if the expected number
    /// of insertions occured.
    async fn insert_operation(
        &self,
        operation: &StorageOperation,
    ) -> Result<bool, OperationStorageError>;

    /// Get an operation identified by it's OperationId.
    ///
    /// Returns a type implementing `AsStorageOperation` which includes `Author`, `DocumentId` and
    /// `OperationId` metadata.
    async fn get_operation_by_id(
        &self,
        id: OperationId,
    ) -> Result<Option<StorageOperation>, OperationStorageError>;

    /// Retrieve the id of the document an operation is contained within.
    ///
    /// If no document was found, then this method returns a result wrapping
    /// a None variant.
    async fn get_document_by_operation_id(
        &self,
        id: OperationId,
    ) -> Result<Option<DocumentId>, OperationStorageError>;

    // /// Get just the fields of an operation, identified by their OperationId.
    // async fn get_operation_fields_by_id(
    //     &self,
    //     id: OperationId,
    // ) -> Result<Option<OperationFields>, OperationStorageError>;

    // async fn get_operation_value_by_id(
    //     &self,
    //     id: OperationId,
    //     name: String,
    // ) -> Result<OperationValue, OperationStorageError>;

    // async fn get_operations_by_document_id(
    //     &self,
    //     id: DocumentId,
    //     name: String,
    // ) -> Result<Vec<StorageOperation>, OperationStorageError>;
}
