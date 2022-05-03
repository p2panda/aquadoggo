// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{OperationAction, OperationFields, OperationId, OperationValue};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::Validate;

use crate::db::db_types::{OperationFieldRow, OperationRow, PreviousOperationRelationRow};
use crate::db::errors::{DocumentViewStorageError, OperationStorageError};

/// The string name of a documents field
pub type FieldName = String;

/// A map associating fields identified by their name with an operation which
/// conatins this fields value(s).
pub type FieldIds = BTreeMap<FieldName, OperationId>;

/// The fields of a document view.
pub type DocumentViewFields = BTreeMap<FieldName, OperationValue>;

/// WIP: Storage trait representing a document view.
pub trait AsStorageDocumentView: Sized + Clone + Send + Sync {
    /// The error type returned by this traits' methods.
    type AsStorageDocumentViewError: 'static + std::error::Error;

    fn id(&self) -> DocumentViewId;

    fn iter(&self) -> Iter<FieldName, OperationValue>;

    fn get(&self, key: &str) -> Option<&OperationValue>;

    fn schema_id(&self) -> SchemaId;

    fn field_ids(&self) -> FieldIds;
}

/// Storage traits for documents and document views.
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

pub type PreviousOperations = Vec<OperationId>;

pub trait AsStorageOperation: Sized + Clone + Send + Sync {
    /// The error type returned by this traits' methods.
    type AsStorageOperationError: 'static + std::error::Error;

    fn action(&self) -> OperationAction;

    fn author(&self) -> Author;

    fn fields(&self) -> Option<OperationFields>;

    fn id(&self) -> OperationId;

    fn previous_operations(&self) -> PreviousOperations;

    fn schema_id(&self) -> SchemaId;
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
