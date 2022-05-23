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
    use std::str::FromStr;

    use p2panda_rs::document::{
        DocumentBuilder, DocumentId, DocumentView, DocumentViewFields, DocumentViewId,
        DocumentViewValue,
    };
    use p2panda_rs::entry::{Entry, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{
        AsOperation, Operation, OperationFields, OperationId, OperationValue, PinnedRelationList,
    };
    use p2panda_rs::schema::system::{SchemaFieldView, SchemaView};
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::storage_provider::traits::{
        AsStorageEntry, EntryStore, OperationStore, StorageProvider,
    };
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;

    use crate::db::stores::test_utils::{construct_publish_entry_request, test_db};
    use crate::db::stores::OperationStorage;
    use crate::db::traits::DocumentStore;
    use crate::graphql::client::PublishEntryRequest;

    use super::SchemaStore;

    #[tokio::test]
    async fn inserts_gets_one_document_view() {
        let (storage_provider, key_pairs, _documents) = test_db(0, 0, false).await;
        let key_pair = KeyPair::new();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

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

        let request = construct_publish_entry_request(
            &storage_provider,
            &schema_name_field_definition_operation,
            &key_pair,
            None,
        )
        .await;

        let operation_id: OperationId = request.entry_encoded.hash().into();
        let document_id: DocumentId = request.entry_encoded.hash().into();
        let document_view_id: DocumentViewId = request.entry_encoded.hash().into();

        storage_provider.publish_entry(&request).await.unwrap();
        storage_provider
            .insert_operation(&OperationStorage::new(
                &author,
                &schema_name_field_definition_operation,
                &operation_id,
                &document_id,
            ))
            .await
            .unwrap();

        let document_view_fields = DocumentViewFields::new_from_operation_fields(
            &operation_id,
            &schema_name_field_definition_operation.fields().unwrap(),
        );
        storage_provider
            .insert_document_view(
                &DocumentView::new(&document_view_id, &document_view_fields),
                &SchemaId::new("schema_field_definition_v1").unwrap(),
            )
            .await
            .unwrap();

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
                OperationValue::PinnedRelationList(PinnedRelationList::new(vec![document_view_id])),
            )
            .unwrap();
        let schema_definition_operation = Operation::new_create(
            SchemaId::new("schema_definition_v1").unwrap(),
            schema_definition_operation_fields,
        )
        .unwrap();

        let request = construct_publish_entry_request(
            &storage_provider,
            &schema_definition_operation,
            &key_pair,
            None,
        )
        .await;

        let operation_id: OperationId = request.entry_encoded.hash().into();
        let document_id: DocumentId = request.entry_encoded.hash().into();
        let document_view_id: DocumentViewId = request.entry_encoded.hash().into();

        storage_provider.publish_entry(&request).await.unwrap();
        storage_provider
            .insert_operation(&OperationStorage::new(
                &author,
                &schema_definition_operation,
                &operation_id,
                &document_id,
            ))
            .await
            .unwrap();

        let document_view_fields = DocumentViewFields::new_from_operation_fields(
            &operation_id,
            &schema_definition_operation.fields().unwrap(),
        );
        storage_provider
            .insert_document_view(
                &DocumentView::new(&document_view_id, &document_view_fields),
                &SchemaId::new("schema_definition_v1").unwrap(),
            )
            .await
            .unwrap();

        let schema = storage_provider
            .get_schema_by_id(&document_view_id)
            .await
            .unwrap();

        println!("{}", schema)
    }
}
