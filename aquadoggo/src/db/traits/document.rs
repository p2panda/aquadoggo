// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::btree_map::Iter;

use async_trait::async_trait;

use p2panda_rs::document::{DocumentViewFields, DocumentViewId, DocumentViewValue};
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

/// Storage traits for documents and document views.
#[async_trait]
pub trait DocumentStore<StorageDocumentView: AsStorageDocumentView> {
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
}
