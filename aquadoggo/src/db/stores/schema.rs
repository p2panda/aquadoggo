// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use async_trait::async_trait;

use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::{
    system::{SchemaFieldView, SchemaView},
    Schema, SchemaId,
};

use crate::db::errors::SchemaStoreError;
use crate::db::traits::SchemaStore;
use crate::db::{provider::SqlStorage, traits::DocumentStore};

#[async_trait]
impl SchemaStore for SqlStorage {
    /// Get a Schema from the database by it's document view id.
    ///
    /// Internally, this method performs three steps:
    /// - fetch the document view for the schema definition
    /// - fetch the document views for every field defined in the schema definition
    /// - combine the returned fields into a Schema struct
    ///
    /// If any of these steps fail then an error is returned, otherwise a completed Schema struct
    /// which can be used to validate application operations is returned.
    async fn get_schema_by_id(&self, id: &DocumentViewId) -> Result<Schema, SchemaStoreError> {
        // Fetch the document view for the schema
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

    /// Get all Schema which have been published to this node.
    ///
    /// Returns an error if a fatal db error occured.
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

        println!("{:#?}", schema_views);
        println!("{:#?}", schema_field_views);

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
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationFields, OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, SchemaId};

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{insert_entry_operation_and_view, test_db};

    use super::SchemaStore;

    async fn create_venue_schema(
        storage_provider: &SqlStorage,
        key_pair: &KeyPair,
    ) -> DocumentViewId {
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
            storage_provider,
            key_pair,
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
            storage_provider,
            key_pair,
            &SchemaId::new("schema_definition_v1").unwrap(),
            None,
            &schema_definition_operation,
        )
        .await;

        document_view_id
    }

    #[tokio::test]
    async fn get_schema() {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;
        let key_pair = KeyPair::new();

        let document_view_id = create_venue_schema(&storage_provider, &key_pair).await;

        // Retrieve the schema by it's document_view_id.
        let schema = storage_provider
            .get_schema_by_id(&document_view_id)
            .await
            .unwrap();

        assert_eq!(schema.as_cddl(), "venue_name = { type: \"str\", value: tstr, }\ncreate-fields = { venue_name }\nupdate-fields = { + ( venue_name ) }")
    }

    #[tokio::test]
    async fn get_all_schema() {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;
        let key_pair = KeyPair::new();

        create_venue_schema(&storage_provider, &key_pair).await;

        let schemas = storage_provider.get_all_schema().await.unwrap();

        assert_eq!(schemas.len(), 1)
    }
}
