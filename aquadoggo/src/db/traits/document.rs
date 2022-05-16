// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;

use async_trait::async_trait;

use p2panda_rs::document::{
    Document, DocumentId, DocumentView, DocumentViewFields, DocumentViewId, DocumentViewValue,
};
use p2panda_rs::schema::SchemaId;

use crate::db::errors::DocumentStorageError;

/// WIP: Storage trait representing a document view.
pub trait AsStorageDocumentView: Sized + Clone + Send + Sync {
    /// The error type returned by this traits' methods.
    type AsStorageDocumentViewError: 'static + std::error::Error;

    fn id(&self) -> &DocumentViewId;

    fn iter(&self) -> Iter<String, DocumentViewValue>;

    fn get(&self, key: &str) -> Option<&DocumentViewValue>;

    fn fields(&self) -> &DocumentViewFields;
}

/// WIP: Storage trait representing a document view.
pub trait AsStorageDocument: Sized + Clone + Send + Sync {
    /// The error type returned by this traits' methods.
    type AsStorageDocumentError: 'static + std::error::Error;

    fn id(&self) -> &DocumentId;

    fn schema_id(&self) -> &SchemaId;

    fn view(&self) -> Option<&DocumentView>;

    fn view_id(&self) -> &DocumentViewId;

    fn is_deleted(&self) -> bool;
}

/// Storage traits for documents and document views.
#[async_trait]
pub trait DocumentStore<
    StorageDocumentView: AsStorageDocumentView,
    StorageDocument: AsStorageDocument,
>
{
    async fn insert_document_view(
        &self,
        document_view: &StorageDocumentView,
        schema_id: &SchemaId,
    ) -> Result<(), DocumentStorageError>;

    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<StorageDocumentView, DocumentStorageError>;

    async fn get_document_views_by_schema(
        &self,
        schema_id: &SchemaId,
    ) -> Result<Vec<StorageDocumentView>, DocumentStorageError>;

    async fn insert_document(&self, document: &StorageDocument)
        -> Result<(), DocumentStorageError>;
}
