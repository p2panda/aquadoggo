// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::system::{SchemaFieldView, SchemaView};
use p2panda_rs::schema::{Schema, SchemaId};

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
    /// If no schema definition with the passed id is found then None is returned,
    /// if any of the other steps can't be completed, then an error is returned.
    async fn get_schema_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<Option<Schema>, SchemaStoreError> {
        // Fetch the document view for the schema
        let schema_view: SchemaView = match self.get_document_view_by_id(id).await? {
            Some(document_view) => document_view.try_into()?,
            None => return Ok(None),
        };

        let mut schema_fields = vec![];

        for field_id in schema_view.fields().iter() {
            // Fetch schema field document views
            let scheme_field_view: SchemaFieldView =
                match self.get_document_view_by_id(&field_id).await? {
                    Some(document_view) => document_view.try_into()?,
                    None => {
                        return Err(SchemaStoreError::MissingSchemaFieldDefinition(
                            field_id,
                            id.to_owned(),
                        ))
                    }
                };

            schema_fields.push(scheme_field_view);
        }

        let schema = Schema::new(schema_view, schema_fields)?;

        Ok(Some(schema))
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
    use p2panda_rs::operation::{OperationFields, OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::test_utils::fixtures::{
        document_view_id, key_pair, operation, operation_fields,
    };
    use rstest::rstest;

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{insert_entry_operation_and_view, test_db};

    use super::SchemaStore;

    async fn insert_schema_definition(
        storage_provider: &SqlStorage,
        key_pair: &KeyPair,
        schema_field_id: &DocumentViewId,
        mut schema_definition: OperationFields,
    ) -> DocumentViewId {
        schema_definition
            .add(
                "fields",
                // This pinned relation points at the previously published field.
                OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                    schema_field_id.clone()
                ])),
            )
            .unwrap();

        let (_, document_view_id) = insert_entry_operation_and_view(
            storage_provider,
            key_pair,
            None,
            &operation(
                Some(schema_definition),
                None,
                Some(SchemaId::new("schema_definition_v1").unwrap()),
            ),
        )
        .await;
        document_view_id
    }

    async fn insert_schema_field_definition(
        storage_provider: &SqlStorage,
        key_pair: &KeyPair,
        schema_field_definition: OperationFields,
    ) -> DocumentViewId {
        // Publish it encoded in an entry, insert the operation and materialised document view into the db
        let (_, document_view_id) = insert_entry_operation_and_view(
            storage_provider,
            key_pair,
            None,
            &operation(
                Some(schema_field_definition),
                None,
                Some(SchemaId::new("schema_field_definition_v1").unwrap()),
            ),
        )
        .await;

        document_view_id
    }

    #[rstest]
    #[case::valid_schema_and_fields(
        "venue_name = { type: \"str\", value: tstr, }\ncreate-fields = { venue_name }\nupdate-fields = { + ( venue_name ) }",
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string())), ("type", FieldType::String.into())]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[should_panic(expected = "missing field \"name\"")]
    #[case::fields_missing_name_field(
        "",
        operation_fields(vec![("type", FieldType::String.into())]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[should_panic(expected = "missing field \"type\"")]
    #[case::fields_missing_type_field(
        "",
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string()))]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[should_panic(expected = "missing field \"name\"")]
    #[case::schema_missing_name_field(
        "",
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string())), ("type", FieldType::String.into())]), 
        operation_fields(vec![("description", OperationValue::Text("My venue".to_string()))]))]
    #[should_panic(expected = "missing field \"description\"")]
    #[case::schema_missing_name_description(
        "",
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string())), ("type", FieldType::String.into())]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string()))]))]
    #[tokio::test]
    async fn get_schema(
        #[case] cddl_str: &str,
        #[case] schema_field_definition: OperationFields,
        #[case] schema_definition: OperationFields,
        key_pair: KeyPair,
    ) {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;

        let document_view_id =
            insert_schema_field_definition(&storage_provider, &key_pair, schema_field_definition)
                .await;

        let document_view_id = insert_schema_definition(
            &storage_provider,
            &key_pair,
            &document_view_id,
            schema_definition,
        )
        .await;

        let schema = storage_provider
            .get_schema_by_id(&document_view_id)
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        assert_eq!(schema.unwrap().as_cddl(), cddl_str)
    }

    #[rstest]
    #[case::works(
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string())), ("type", FieldType::String.into())]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[should_panic(expected = "invalid fields found for this schema")]
    #[case::does_not_work(
        operation_fields(vec![("name", OperationValue::Text("venue_name".to_string()))]), 
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[tokio::test]
    async fn get_all_schema(
        #[case] schema_field_definition: OperationFields,
        #[case] schema_definition: OperationFields,
        key_pair: KeyPair,
    ) {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;

        let document_view_id =
            insert_schema_field_definition(&storage_provider, &key_pair, schema_field_definition)
                .await;

        insert_schema_definition(
            &storage_provider,
            &key_pair,
            &document_view_id,
            schema_definition,
        )
        .await;

        let schemas = storage_provider
            .get_all_schema()
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        assert_eq!(schemas.len(), 1)
    }

    #[rstest]
    #[case::schema_fields_do_not_exist(
        operation_fields(vec![("name", OperationValue::Text("venue".to_string())), ("description", OperationValue::Text("My venue".to_string()))]))]
    #[tokio::test]
    async fn schema_fields_do_not_exist(
        #[case] schema_definition: OperationFields,
        #[from(document_view_id)] schema_fields_id: DocumentViewId,
        key_pair: KeyPair,
    ) {
        let (storage_provider, _key_pairs, _documents) = test_db(0, 0, false).await;

        let document_view_id = insert_schema_definition(
            &storage_provider,
            &key_pair,
            &schema_fields_id,
            schema_definition,
        )
        .await;

        // Retrieve the schema by it's document_view_id.
        let schema = storage_provider.get_schema_by_id(&document_view_id).await;

        assert_eq!(schema.unwrap_err().to_string(), format!("No document view found for schema field definition with id: {0} which is required by schema definition {1}", schema_fields_id, document_view_id))
    }
}
