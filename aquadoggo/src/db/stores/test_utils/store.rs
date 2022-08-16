// SPDX-License-Identifier: AGPL-3.0-or-later

use log::{debug, info};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{OperationBuilder, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::traits::StorageProvider;
use p2panda_rs::test_utils::db::test_db::send_to_store;

use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::TaskInput;
use crate::{Configuration, SchemaProvider};

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase<S: StorageProvider = SqlStorage> {
    pub context: Context<S>,
    pub store: S,
    pub test_data: TestData,
}

impl<S: StorageProvider + Clone> TestDatabase<S> {
    pub fn new(store: S) -> Self {
        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        TestDatabase {
            context,
            store,
            test_data: TestData::default(),
        }
    }
}

impl TestDatabase {
    /// Publish a document and materialise it in the store.
    ///
    /// Also runs dependency task for document.
    pub async fn add_document(
        &mut self,
        schema_id: &SchemaId,
        fields: Vec<(&str, OperationValue)>,
        key_pair: &KeyPair,
    ) -> DocumentViewId {
        info!("Creating document for {}", schema_id);

        // Get requested schema from store.
        let schema = self
            .context
            .schema_provider
            .get(schema_id)
            .await
            .expect("Schema not found");

        // Build, publish and reduce create operation for document.
        let create_op = OperationBuilder::new(schema.id())
            .fields(&fields)
            .build()
            .expect("Build operation");

        let (entry_signed, _) = send_to_store(&self.store, &create_op, &schema, key_pair)
            .await
            .expect("Publish CREATE operation");

        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        let dependency_tasks = reduce_task(self.context.clone(), input.clone())
            .await
            .expect("Reduce document");

        // Run dependency tasks
        if let Some(tasks) = dependency_tasks {
            for task in tasks {
                dependency_task(self.context.clone(), task.input().to_owned())
                    .await
                    .expect("Run dependency task");
            }
        }
        DocumentViewId::from(entry_signed.hash())
    }

    /// Publish a schema and materialise it in the store.
    pub async fn add_schema(
        &mut self,
        name: &str,
        fields: Vec<(&str, FieldType)>,
        key_pair: &KeyPair,
    ) -> Schema {
        info!("Creating schema {}", name);
        let mut field_ids = Vec::new();

        // Build and reduce schema field definitions
        for field in fields {
            let create_field_op = Schema::create_field(field.0, field.1.clone());
            let (entry_signed, _) = send_to_store(
                &self.store,
                &create_field_op,
                &Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap(),
                key_pair,
            )
            .await
            .expect("Publish schema fields");

            let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
            reduce_task(self.context.clone(), input).await.unwrap();

            info!("Added field '{}' ({})", field.0, field.1);
            field_ids.push(DocumentViewId::from(entry_signed.hash()));
        }

        // Build and reduce schema definition
        let create_schema_op = Schema::create(name, "test schema description", field_ids);
        let (entry_signed, _) = send_to_store(
            &self.store,
            &create_schema_op,
            &Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
            key_pair,
        )
        .await
        .expect("Publish schema");

        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        reduce_task(self.context.clone(), input.clone())
            .await
            .expect("Reduce schema document");

        // Run schema task for this spec
        let input = TaskInput::new(None, Some(DocumentViewId::from(entry_signed.hash())));
        schema_task(self.context.clone(), input)
            .await
            .expect("Run schema task");

        let view_id = DocumentViewId::from(entry_signed.hash());
        let schema_id = SchemaId::Application(name.to_string(), view_id);

        debug!("Done building {}", schema_id);
        self.context
            .schema_provider
            .get(&schema_id)
            .await
            .expect("Failed adding schema to provider.")
    }
}

/// Data collected when populating a `TestDatabase` in order to easily check values which
/// would be otherwise hard or impossible to get through the store methods.
#[derive(Default)]
pub struct TestData {
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}
