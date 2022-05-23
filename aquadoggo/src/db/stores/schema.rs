// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use async_trait::async_trait;
use p2panda_rs::{
    document::{DocumentView, DocumentViewId},
    schema::{
        system::{SchemaFieldView, SchemaView, SystemSchemaError},
        Schema, SchemaError,
    },
};

use crate::db::{
    models::document::DocumentViewFieldRow, provider::SqlStorage,
    utils::parse_document_view_field_rows,
};

/// `SchemaStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum SchemaStoreError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    Custom(String),

    /// Error returned from converting p2panda-rs `DocumentView` into `SchemaView.
    #[error(transparent)]
    SystemSchemaError(#[from] SystemSchemaError),

    /// Error returned from converting p2panda-rs `DocumentView` into `SchemaView.
    #[error(transparent)]
    SchemaError(#[from] SchemaError),
}

#[async_trait]
pub trait SchemaStore {
    async fn get_schema_by_id(&self, id: &DocumentViewId) -> Result<Schema, SchemaStoreError>;
}

#[async_trait]
impl SchemaStore for SqlStorage {
    async fn get_schema_by_id(&self, id: &DocumentViewId) -> Result<Schema, SchemaStoreError> {
        // Fetch the document view rows for each of the schema
        let schema_rows: Vec<DocumentViewFieldRow> = vec![];
        let schema_view: SchemaView =
            DocumentView::new(id, &parse_document_view_field_rows(schema_rows)).try_into()?;

        // Fetch the document view rows for each of the schema fields
        let mut schema_fields: Vec<SchemaFieldView> = vec![];

        for field in schema_view.fields().iter() {
            // Fetch schema field document views
            let schema_field_rows: Vec<DocumentViewFieldRow> = vec![];
            let scheme_field_view: SchemaFieldView =
                DocumentView::new(id, &parse_document_view_field_rows(schema_field_rows))
                    .try_into()?;
            schema_fields.push(scheme_field_view);
        }

        let schema = Schema::new(schema_view, schema_fields)?;

        Ok(schema)
    }
}
