// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;

use async_trait::async_trait;

use p2panda_rs::document::{
    DocumentId, DocumentView, DocumentViewFields, DocumentViewId, DocumentViewValue,
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
    /// Insert document view into storage.
    ///
    /// The passed view must implement the `AsStorageDocumentView` trait. Errors when
    /// a fatal storage error occurs.
    async fn insert_document_view(
        &self,
        document_view: &StorageDocumentView,
        schema_id: &SchemaId,
    ) -> Result<(), DocumentStorageError>;

    /// Get a document view from storage by it's `DocumentViewId`.
    ///
    /// Returns a type implementing `AsStorageDocumentView` wrapped in an `Option`, returns
    /// `None` if no view was found with this id. Returns an error if a fatal storage error
    /// occured.
    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<Option<StorageDocumentView>, DocumentStorageError>;

    /// Insert a document into storage.
    ///
    /// Inserts a document into storage and should retain a pointer to it's most recent
    /// document view. Returns an error if a fatal storage error occured.
    async fn insert_document(&self, document: &StorageDocument)
        -> Result<(), DocumentStorageError>;

    /// Get the lates document view for a document identified by it's `DocumentId`.
    ///
    /// Returns a type implementing `AsStorageDocumentView` wrapped in an `Option`, returns
    /// `None` if no view was found with this document. Returns an error if a fatal storage error
    /// occured.
    ///
    /// Note: if no view for this document was found, it might have been deleted.
    async fn get_document_by_id(
        &self,
        id: &DocumentId,
    ) -> Result<Option<StorageDocumentView>, DocumentStorageError>;

    /// Get the most recent view for all documents which follow the passed schema.
    ///
    /// Returns a vector of `DocumentView`, or an empty vector if none were found. Returns
    /// an error when a fatal storage error occured.  
    async fn get_documents_by_schema(
        &self,
        schema_id: &SchemaId,
    ) -> Result<Vec<StorageDocumentView>, DocumentStorageError>;
}
