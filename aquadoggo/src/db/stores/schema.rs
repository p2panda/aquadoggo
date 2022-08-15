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
                    None => return Ok(None),
                };

            schema_fields.push(scheme_field_view);
        }

        // We silently ignore errors as we are assuming views we retrieve from the database
        // themselves are valid, meaning any error in constructing the schema must be because
        // some of it's fields are simply missing from our database.
        let schema = Schema::from_views(schema_view, schema_fields).ok();

        Ok(schema)
    }

    /// Get all Schema which have been published to this node.
    ///
    /// Returns an error if a fatal db error occured.
    ///
    /// Silently ignores incomplete or broken schema definitions.
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
                .filter_map(|field_id| schema_field_views.iter().find(|view| view.id() == field_id))
                .map(|field| field.to_owned())
                .collect();

            all_schema.push(Schema::from_views(schema_view, schema_fields).ok());
        }

        Ok(all_schema.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationFields, OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::test_utils::fixtures::{
        document_view_id, key_pair, operation, operation_fields, schema_fields,
    };
    use rstest::rstest;

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{
        insert_entry_operation_and_view, test_db, TestDatabase, TestDatabaseRunner,
    };

    use super::SchemaStore;

    async fn insert_schema_definition(
        storage_provider: &SqlStorage,
        key_pair: &KeyPair,
        schema_field_id: &DocumentViewId,
        mut schema_definition: OperationFields,
    ) -> DocumentViewId {
        schema_definition
            .insert(
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
                SchemaId::new("schema_definition_v1").unwrap(),
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
                SchemaId::new("schema_field_definition_v1").unwrap(),
            ),
        )
        .await;

        document_view_id
    }

    #[rstest]
    #[case::fields_missing_name_field("missing field \"name\"",
        operation_fields(vec![
                         ("type", FieldType::String.into())
        ]),
        operation_fields(vec![
                         ("name", OperationValue::String("venue".to_string())),
                         ("description", OperationValue::String("My venue".to_string()))
        ])
    )]
    #[case::fields_missing_type_field("missing field \"type\"",
        operation_fields(vec![
                         ("name", OperationValue::String("venue_name".to_string()))
        ]),
        operation_fields(vec![
                         ("name", OperationValue::String("venue".to_string())),
                         ("description", OperationValue::String("My venue".to_string()))
        ])
    )]
    #[case::schema_missing_name_field("missing field \"name\"",
        operation_fields(vec![
                         ("name", OperationValue::String("venue_name".to_string())),
                         ("type", FieldType::String.into())
        ]),
        operation_fields(vec![
                         ("description", OperationValue::String("My venue".to_string()))
        ])
    )]
    #[case::schema_missing_name_description("missing field \"description\"",
        operation_fields(vec![
                         ("name", OperationValue::String("venue_name".to_string())),
                         ("type", FieldType::String.into())
        ]),
        operation_fields(vec![
                         ("name", OperationValue::String("venue".to_string()))
        ])
    )]
    fn get_schema_errors(
        #[case] err_str: &str,
        #[case] schema_field_definition: OperationFields,
        #[case] schema_definition: OperationFields,
        key_pair: KeyPair,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        let err_str = err_str.to_string();

        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_view_id =
                insert_schema_field_definition(&db.store, &key_pair, schema_field_definition).await;

            let document_view_id = insert_schema_definition(
                &db.store,
                &key_pair,
                &document_view_id,
                schema_definition,
            )
            .await;

            let schema = db.store.get_schema_by_id(&document_view_id).await;

            assert_eq!(schema.unwrap_err().to_string(), err_str);
        });
    }

    #[rstest]
    #[case(
        vec![
            ("venue", FieldType::String),
            ("address", FieldType::String)
        ],
        1
    )]
    fn get_all_schema(
        #[case] schema_definition: Vec<(&'static str, FieldType)>,
        #[case] expected_schema_count: usize,
        key_pair: KeyPair,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |mut db: TestDatabase| async move {
            let schema = db
                .add_schema("test_schema", schema_definition, &key_pair)
                .await;

            // For later...
            // let id = match schema.id() {
            //     SchemaId::Application(name, id) => id,
            //     _ => panic!("Not interested in this"),
            // };

            let schemas = db.store.get_all_schema().await;

            assert_eq!(schemas.unwrap().len(), expected_schema_count);
        });
    }

    #[rstest]
    #[case::schema_fields_do_not_exist(
        operation_fields(vec![
                         ("name", OperationValue::String("venue".to_string())),
                         ("description", OperationValue::String("My venue".to_string()))
        ])
    )]
    fn schema_fields_do_not_exist(
        #[case] schema_definition: OperationFields,
        #[from(document_view_id)] schema_fields_id: DocumentViewId,
        #[from(test_db)] runner: TestDatabaseRunner,
        key_pair: KeyPair,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let document_view_id = insert_schema_definition(
                &db.store,
                &key_pair,
                &schema_fields_id,
                schema_definition,
            )
            .await;

            // Retrieve the schema by it's document_view_id.

            // We unwrap here as we expect an `Ok` result even though the schema could not be built.
            let schema = db.store.get_schema_by_id(&document_view_id).await.unwrap();

            assert!(schema.is_none());
        });
    }
}
