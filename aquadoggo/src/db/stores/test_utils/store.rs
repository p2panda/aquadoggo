// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use log::{debug, info};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{EncodedEntry, Entry};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::{
    EncodedOperation, Operation, OperationBuilder, OperationFields, OperationValue,
};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::traits::StorageProvider;
use p2panda_rs::test_utils::constants::{test_fields, SCHEMA_ID};
use p2panda_rs::test_utils::fixtures::{operation, operation_fields};

use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::db::stores::test_utils::test_key_pairs;
use crate::domain::{next_args, publish};
use crate::graphql::client::NextEntryArguments;
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
            .unwrap();
        let (entry_signed, _) = send_to_store(&self.store, &create_op, None, key_pair).await;
        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        let dependency_tasks = reduce_task(self.context.clone(), input.clone())
            .await
            .unwrap();

        // Run dependency tasks
        if let Some(tasks) = dependency_tasks {
            for task in tasks {
                dependency_task(self.context.clone(), task.input().to_owned())
                    .await
                    .unwrap();
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
            let (entry_signed, _) =
                send_to_store(&self.store, &create_field_op, None, key_pair).await;

            let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
            reduce_task(self.context.clone(), input).await.unwrap();

            info!("Added field '{}' ({})", field.0, field.1);
            field_ids.push(DocumentViewId::from(entry_signed.hash()));
        }

        // Build and reduce schema definition
        let create_schema_op = Schema::create(name, "test schema description", field_ids);
        let (entry_signed, _) = send_to_store(&self.store, &create_schema_op, None, key_pair).await;
        let input = TaskInput::new(None, Some(DocumentViewId::from(entry_signed.hash())));
        reduce_task(self.context.clone(), input.clone())
            .await
            .unwrap();

        // Run schema task for this spec
        schema_task(self.context.clone(), input).await.unwrap();

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

/// Configuration used when populating a `TestDatabase`.
pub struct PopulateDatabaseConfig {
    /// Number of entries per log/document.
    pub no_of_entries: usize,

    /// Number of logs for each author.
    pub no_of_logs: usize,

    /// Number of authors, each with logs populated as defined above.
    pub no_of_authors: usize,

    /// A boolean flag for wether all logs should contain a delete operation.
    pub with_delete: bool,

    /// The schema used for all operations in the db.
    pub schema: SchemaId,

    /// The fields used for every CREATE operation.
    pub create_operation_fields: Vec<(&'static str, OperationValue)>,

    /// The fields used for every UPDATE operation.
    pub update_operation_fields: Vec<(&'static str, OperationValue)>,
}

impl Default for PopulateDatabaseConfig {
    fn default() -> Self {
        Self {
            no_of_entries: 0,
            no_of_logs: 0,
            no_of_authors: 0,
            with_delete: false,
            schema: SCHEMA_ID.parse().unwrap(),
            create_operation_fields: test_fields(),
            update_operation_fields: test_fields(),
        }
    }
}

/// Helper method for populating a `TestDatabase` with configurable data.
///
/// Passed parameters define what the db should contain. The first entry in each log contains a
/// valid CREATE operation following entries contain duplicate UPDATE operations. If the
/// with_delete flag is set to true the last entry in all logs contain be a DELETE operation.
pub async fn populate_test_db<S: StorageProvider>(
    db: &mut TestDatabase<S>,
    config: &PopulateDatabaseConfig,
) {
    let key_pairs = test_key_pairs(config.no_of_authors);

    for key_pair in &key_pairs {
        db.test_data
            .key_pairs
            .push(KeyPair::from_private_key(key_pair.private_key()).unwrap());

        for _log_id in 0..config.no_of_logs {
            let mut document_id: Option<DocumentId> = None;
            let mut previous_operation: Option<DocumentViewId> = None;

            for index in 0..config.no_of_entries {
                // Create an operation based on the current index and whether this document should
                // contain a DELETE operation
                let next_operation_fields = match index {
                    // First operation is CREATE
                    0 => Some(operation_fields(config.create_operation_fields.clone())),
                    // Last operation is DELETE if the with_delete flag is set
                    seq if seq == (config.no_of_entries - 1) && config.with_delete => None,
                    // All other operations are UPDATE
                    _ => Some(operation_fields(config.update_operation_fields.clone())),
                };

                // Publish the operation encoded on an entry to storage.
                let (entry_encoded, publish_entry_response) = send_to_store::<S>(
                    &db.store,
                    &operation(
                        next_operation_fields,
                        previous_operation,
                        config.schema.to_owned(),
                    ),
                    document_id.as_ref(),
                    key_pair,
                )
                .await;

                // Set the previous_operations based on the backlink
                previous_operation = publish_entry_response.backlink.map(DocumentViewId::from);

                // If this was the first entry in the document, store the doucment id for later.
                if index == 0 {
                    document_id = Some(entry_encoded.hash().into());
                    db.test_data.documents.push(document_id.clone().unwrap());
                }
            }
        }
    }
}

/// Helper method for publishing an operation encoded on an entry to a store.
pub async fn send_to_store<S: StorageProvider>(
    store: &S,
    operation: &Operation,
    document_id: Option<&DocumentId>,
    key_pair: &KeyPair,
) -> (EncodedEntry, NextEntryArguments) {
    // TODO: Needed full refactor
    todo!()
}
