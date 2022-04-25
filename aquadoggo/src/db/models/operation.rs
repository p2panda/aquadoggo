use async_trait::async_trait;
// SPDX-License-Identifier: AGPL-3.0-or-later
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{OperationFields, OperationId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;

/////// DB ROWS ///////
// Structs representing data as it is stored in rows in the db
pub struct OperationRow {
    id: String,
    author: String,
    entry_hash: String,
    schema_id_hash: String,
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

////// OPERATION STORAGE TRAITS ///////

pub trait AsStorageOperation: Sized + Clone + Send + Sync + Validate {
    /// The error type returned by this traits' methods.
    type AsStorageOperationError: 'static + std::error::Error;

    fn author(&self) -> Author;

    fn id(&self) -> OperationId;

    fn schema_id(&self) -> SchemaId;

    fn fields(&self) -> OperationFields;

    fn previous_operations(&self) -> Vec<OperationId>;

    fn document_id(&self) -> DocumentId;

    // We could store relations between document view id hashes
    // and the operation id graph tips in order to reconstruct the
    // complete DocumentViewId if it's needed (is it needed?).
    fn document_view_id(&self) -> DocumentViewId;

    fn document_view_id_hash(&self) -> DocumentViewIdHash;
}

pub struct OperationStoreError {}

#[async_trait]
pub trait OperationStore<StorageOperation: AsStorageOperation> {
    async fn insert_operation(
        &self,
        operation: &StorageOperation,
    ) -> Result<bool, OperationStoreError>;

    async fn insert_previous_operation_relation(
        &self,
        child: &OperationId,
        parent: &OperationId,
    ) -> Result<bool, OperationStoreError>;

    async fn insert_operation_fields(
        &self,
        fields: &OperationFields,
        document_relation: &DocumentId,
        document_view_relation: &DocumentViewIdHash,
    ) -> Result<bool, OperationStoreError>;

    // Helper method for inserting all operation relation dependencies
    async fn insert_operation_relations(
        &self,
        operation: &StorageOperation,
    ) -> Result<bool, OperationStoreError> {
        for previous_operation in operation.previous_operations() {
            self.insert_previous_operation_relation(&operation.id(), &previous_operation)
                .await?;
        }
        self.insert_operation_fields(
            &operation.fields(),
            &operation.document_id(),
            &operation.document_view_id_hash(),
        )
        .await?;
        Ok(true)
    }

    async fn get_operation(&self, id: OperationId)
        -> Result<StorageOperation, OperationStoreError>;
}
