// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use async_trait::async_trait;
use p2panda_rs::{
    document::{DocumentView, DocumentViewId},
    schema::{
        system::{SchemaFieldView, SchemaView, SystemSchemaError},
        Schema, SchemaError, SchemaId, SchemaIdError,
    },
};

use crate::db::{
    errors::DocumentStorageError, models::document::DocumentViewFieldRow, provider::SqlStorage,
    traits::DocumentStore, utils::parse_document_view_field_rows,
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

    /// Error returned from p2panda-rs `Schema` methods.
    #[error(transparent)]
    SchemaError(#[from] SchemaError),

    /// Error returned from p2panda-rs `Schema` methods.
    #[error(transparent)]
    SchemaIdError(#[from] SchemaIdError),

    /// Error returned from `DocumentStore` methods.
    #[error(transparent)]
    DocumentStorageError(#[from] DocumentStorageError),
}

#[async_trait]
pub trait SchemaStore {
    async fn get_schema_by_id(&self, id: &DocumentViewId) -> Result<Schema, SchemaStoreError>;

    async fn get_all_schema(&self) -> Result<Vec<Schema>, SchemaStoreError>;
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

    async fn get_all_schema(&self) -> Result<Vec<Schema>, SchemaStoreError> {
        let schema_views: Vec<SchemaView> = self
            .get_documents_by_schema(&SchemaId::new("schema_definition_v1")?)
            .await?
            .into_iter()
            .filter_map(|view| SchemaView::try_from(view).ok())
            .collect();

        let schema_field_views: Vec<SchemaFieldView> = self
            .get_documents_by_schema(&SchemaId::new("schema_field_definition_v1")?)
            .await?
            .into_iter()
            .filter_map(|view| SchemaFieldView::try_from(view).ok())
            .collect();

        let mut all_schema = vec![];

        for schema_view in schema_views {
            let schema_fields: Vec<SchemaFieldView> = schema_view
                .fields()
                .iter()
                .filter_map(|field_id| {
                    schema_field_views
                        .iter()
                        .find(|view| view.id() == &field_id)
                })
                .map(|field| field.to_owned())
                .collect();

            all_schema.push(Schema::new(schema_view, schema_fields)?);
        }

        Ok(all_schema)
    }
}
