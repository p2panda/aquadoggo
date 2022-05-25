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
        let schema_document_view = self.get_document_view_by_id(id).await?.unwrap(); // Don't unwrap here.
        let schema_view: SchemaView = schema_document_view.try_into()?;

        let mut schema_fields = vec![];

        for field in schema_view.fields().iter() {
            // Fetch schema field document views
            let scheme_field_document_view = self.get_document_view_by_id(&field).await?.unwrap(); // Don't unwrap here.

            let scheme_field_view: SchemaFieldView = scheme_field_document_view.try_into()?;
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

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewFields, DocumentViewId};

    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{
        AsOperation, Operation, OperationFields, OperationId, OperationValue, PinnedRelationList,
    };
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::storage_provider;
    use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};

    use crate::db::stores::test_utils::{
        construct_publish_entry_request, insert_entry_operation_and_view, test_db,
    };
    use crate::db::stores::OperationStorage;
    use crate::db::traits::DocumentStore;

    use super::SchemaStore;

    #[tokio::test]
    async fn get_schema() {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;
        let key_pair = KeyPair::new();

        // Construct a CREATE operation for the field of the schema we want to publish
        let mut schema_name_field_definition_operation_fields = OperationFields::new();
        schema_name_field_definition_operation_fields
            .add("name", OperationValue::Text("venue_name".to_string()))
            .unwrap();
        schema_name_field_definition_operation_fields
            .add("type", FieldType::String.into())
            .unwrap();
        let schema_name_field_definition_operation = Operation::new_create(
            SchemaId::new("schema_field_definition_v1").unwrap(),
            schema_name_field_definition_operation_fields,
        )
        .unwrap();

        // Publish it encoded in an entry, insert the operation and materialised document view into the db
        let (_document_id, document_view_id) = insert_entry_operation_and_view(
            &storage_provider,
            &key_pair,
            &SchemaId::new("schema_field_definition_v1").unwrap(),
            None,
            &schema_name_field_definition_operation,
        )
        .await;

        // Construct a CREATE operation for the schema definition we want to publish.
        let mut schema_definition_operation_fields = OperationFields::new();
        schema_definition_operation_fields
            .add("name", OperationValue::Text("venue".to_string()))
            .unwrap();
        schema_definition_operation_fields
            .add("description", OperationValue::Text("My venue".to_string()))
            .unwrap();
        schema_definition_operation_fields
            .add(
                "fields",
                // This pinned relation points at the previously published field.
                OperationValue::PinnedRelationList(PinnedRelationList::new(vec![document_view_id])),
            )
            .unwrap();
        let schema_definition_operation = Operation::new_create(
            SchemaId::new("schema_definition_v1").unwrap(),
            schema_definition_operation_fields,
        )
        .unwrap();

        // Publish it encoded in an entry, insert the operation and materialised document view into the db
        let (_document_id, document_view_id) = insert_entry_operation_and_view(
            &storage_provider,
            &key_pair,
            &SchemaId::new("schema_definition_v1").unwrap(),
            None,
            &schema_definition_operation,
        )
        .await;

        // Retrieve the schema by it's document_view_id.
        let schema = storage_provider
            .get_schema_by_id(&document_view_id)
            .await
            .unwrap();

        assert_eq!(schema.as_cddl(), "venue_name = { type: \"str\", value: tstr, }\ncreate-fields = { venue_name }\nupdate-fields = { + ( venue_name ) }")
    }
}
