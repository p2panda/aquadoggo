// SPDX-License-Identifier: AGPL-3.0-or-later

use log::{info, debug};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{OperationValue, OperationBuilder};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::test_utils::memory_store::helpers::{populate_store, PopulateStoreConfig, send_to_store};

use crate::config::Configuration;
use crate::context::Context;
use crate::db::SqlStore;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::TaskInput;
use crate::schema::SchemaProvider;

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestNode {
    pub context: Context<SqlStore>,
    pub store: SqlStore,
}

impl TestNode {
    pub fn new(store: SqlStore) -> Self {
        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        TestNode { context, store }
    }
}

pub async fn populate_and_materialize(
    node: &mut TestNode,
    config: &PopulateStoreConfig,
) -> (Vec<KeyPair>, Vec<DocumentId>) {
    let (key_pairs, document_ids) = populate_store(&node.store, config).await;

    let schema_name = config.schema.name();
    let schema_fields: Vec<(&str, FieldType)> = config
        .schema
        .fields()
        .iter()
        .map(|(name, field)| (name.as_str(), field.clone()))
        .collect();

    add_schema(
        node,
        schema_name,
        schema_fields,
        key_pairs
            .get(0)
            .expect("There should be at least one key pair"),
    )
    .await;

    node.context
        .schema_provider
        .update(config.schema.clone())
        .await;

    for document_id in document_ids.clone() {
        let input = TaskInput::new(Some(document_id), None);
        let dependency_tasks = reduce_task(node.context.clone(), input.clone())
            .await
            .expect("Reduce document");

        // Run dependency tasks
        if let Some(tasks) = dependency_tasks {
            for task in tasks {
                dependency_task(node.context.clone(), task.input().to_owned())
                    .await
                    .expect("Run dependency task");
            }
        }
    }
    (key_pairs, document_ids)
}


/// Publish a document and materialise it in a given `TestNode`.
///
/// Also runs dependency task for document.
pub async fn add_document(
    node: &mut TestNode,
    schema_id: &SchemaId,
    fields: Vec<(&str, OperationValue)>,
    key_pair: &KeyPair,
) -> DocumentViewId {
    info!("Creating document for {}", schema_id);

    // Get requested schema from store.
    let schema = node
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

    let (entry_signed, _) = send_to_store(&node.store, &create_op, &schema, key_pair)
        .await
        .expect("Publish CREATE operation");

    let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
    let dependency_tasks = reduce_task(node.context.clone(), input.clone())
        .await
        .expect("Reduce document");

    // Run dependency tasks
    if let Some(tasks) = dependency_tasks {
        for task in tasks {
            dependency_task(node.context.clone(), task.input().to_owned())
                .await
                .expect("Run dependency task");
        }
    }
    DocumentViewId::from(entry_signed.hash())
}

/// Publish a schema and materialise it in a given `TestNode`.
pub async fn add_schema(
    node: &mut TestNode,
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
            &node.store,
            &create_field_op,
            Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap(),
            key_pair,
        )
        .await
        .expect("Publish schema fields");

        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        reduce_task(node.context.clone(), input).await.unwrap();

        info!("Added field '{}' ({})", field.0, field.1);
        field_ids.push(DocumentViewId::from(entry_signed.hash()));
    }

    // Build and reduce schema definition
    let create_schema_op = Schema::create(name, "test schema description", field_ids);
    let (entry_signed, _) = send_to_store(
        &node.store,
        &create_schema_op,
        Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
        key_pair,
    )
    .await
    .expect("Publish schema");

    let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
    reduce_task(node.context.clone(), input.clone())
        .await
        .expect("Reduce schema document");

    // Run schema task for this spec
    let input = TaskInput::new(None, Some(DocumentViewId::from(entry_signed.hash())));
    schema_task(node.context.clone(), input)
        .await
        .expect("Run schema task");

    let view_id = DocumentViewId::from(entry_signed.hash());
    let schema_id = SchemaId::Application(name.to_string(), view_id);

    debug!("Done building {}", schema_id);
    node
        .context
        .schema_provider
        .get(&schema_id)
        .await
        .expect("Failed adding schema to provider.")
}
