// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::{OperationFields, OperationId, OperationValue};
use p2panda_rs::schema::SchemaId;

use crate::db::errors::DocumentStorageError;
use crate::db::stores::operation::OperationStorage;

/// The string name of a documents field
pub type FieldName = String;

/// A map associating fields identified by their name with an operation which
/// conatins this fields value(s).
pub type FieldIds = BTreeMap<FieldName, OperationId>;

/// The fields of a document view.
pub type DocumentViewFields = OperationFields;

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
    ) -> Result<bool, DocumentStorageError>;

    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<OperationStorage, DocumentStorageError>;
}
