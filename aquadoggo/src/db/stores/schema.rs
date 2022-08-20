// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};

use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::system::{SchemaFieldView, SchemaView};
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::errors::SchemaStoreError;
use crate::db::provider::SqlStorage;
use crate::db::traits::SchemaStore;

#[async_trait]
impl SchemaStore for SqlStorage {
    /// Get a Schema from the database by it's document view id.
    ///
    /// Internally, this method performs three steps:
    /// - fetch the document view for the schema definition
    /// - fetch the document views for every field defined in the schema definition
    /// - combine the returned fields into a Schema struct
    ///
    /// If no schema definition with the passed id is found then None is returned, if any of the
    /// other steps can't be completed, then an error is returned.
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
                match self.get_document_view_by_id(field_id).await? {
                    Some(document_view) => document_view.try_into()?,
                    None => return Ok(None),
                };

            schema_fields.push(scheme_field_view);
        }

        // We silently ignore errors as we are assuming views we retrieve from the database
        // themselves are valid, meaning any error in constructing the schema must be because some
        // of it's fields are simply missing from our database.
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
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::{FieldType, SchemaId};
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_view_id};
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    use super::SchemaStore;

    #[rstest]
    fn get_schema(key_pair: KeyPair, #[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |mut db: TestDatabase| async move {
            let schema = db
                .add_schema(
                    "test_schema",
                    vec![
                        ("description", FieldType::String),
                        ("profile_name", FieldType::String),
                    ],
                    &key_pair,
                )
                .await;

            let document_view_id = match schema.id() {
                SchemaId::Application(_, view_id) => view_id,
                _ => panic!("Invalid schema id"),
            };

            let result = db
                .store
                .get_schema_by_id(document_view_id)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(result.id(), schema.id());
        });
    }

    #[rstest]
    fn get_all_schema(key_pair: KeyPair, #[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |mut db: TestDatabase| async move {
            for i in 0..5 {
                db.add_schema(
                    &format!("test_schema_{}", i),
                    vec![
                        ("description", FieldType::String),
                        ("profile_name", FieldType::String),
                    ],
                    &key_pair,
                )
                .await;
            }

            let schemas = db.store.get_all_schema().await;
            assert_eq!(schemas.unwrap().len(), 5);
        });
    }

    #[rstest]
    fn schema_fields_do_not_exist(#[from(test_db)] runner: TestDatabaseRunner, key_pair: KeyPair) {
        runner.with_db_teardown(|mut db: TestDatabase| async move {
            // Create a schema definition but no schema field definitions
            let document_view_id = db
                .add_document(
                    &SchemaId::SchemaDefinition(1),
                    vec![
                        ("name", "test_schema".into()),
                        ("description", "My schema without fields".into()),
                        ("fields", vec![random_document_view_id()].into()),
                    ],
                    &key_pair,
                )
                .await;

            // Retrieve the schema by it's document view id. We unwrap here as we expect an `Ok`
            // result for the succeeding db query, even though the schema could not be built.
            let schema = db.store.get_schema_by_id(&document_view_id).await.unwrap();

            // We receive nothing as the fields are missing for this schema
            assert!(schema.is_none());
        });
    }
}
