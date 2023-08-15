// SPDX-License-Identifier: AGPL-3.0-or-later

use log::{debug, info};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{OperationAction, OperationBuilder, OperationId, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId, SchemaName};
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::test_utils::memory_store::helpers::{
    populate_store, send_to_store, PopulateStoreConfig,
};
use rstest::fixture;
use sqlx::query_scalar;

use crate::context::Context;
use crate::db::SqlStore;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::TaskInput;
use crate::test_utils::{doggo_fields, doggo_schema};

/// Test node which contains a context with an [`SqlStore`].
pub struct TestNode {
    pub context: Context<SqlStore>,
}

/// Fixture for constructing a `PopulateStoreConfig` with default values for aquadoggo tests.
///
/// Is passed to `p2panda_rs::test_utils::populate_store` or
/// `crate::test_utils::populate_and_materialize` to populate a store or node with the specified
/// values.
///
/// Passed parameters define what the we want the store to contain. The first entry in each log
/// contains a valid CREATE operation following entries contain UPDATE operations. If the
/// with_delete flag is set to true the last entry in all logs will contain a DELETE operation.
///
/// When using the above methods, each inserted log will contain operations from a single document.
/// When materialized a document will be created for each log.
#[fixture]
pub fn populate_store_config(
    // Number of entries per log/document
    #[default(0)] no_of_entries: usize,

    // Number of logs for each public key
    #[default(0)] no_of_logs: usize,

    // Number of authors, each with logs populated as defined above
    #[default(0)] no_of_public_keys: usize,

    // A boolean flag for wether all logs should contain a delete operation
    #[default(false)] with_delete: bool,

    // The schema used for all operations in the db
    #[default(doggo_schema())] schema: Schema,

    // The fields used for every CREATE operation
    #[default(doggo_fields())] create_operation_fields: Vec<(&'static str, OperationValue)>,

    // The fields used for every UPDATE operation
    #[default(doggo_fields())] update_operation_fields: Vec<(&'static str, OperationValue)>,
) -> PopulateStoreConfig {
    PopulateStoreConfig {
        no_of_entries,
        no_of_logs,
        no_of_public_keys,
        with_delete,
        schema,
        create_operation_fields,
        update_operation_fields,
    }
}

/// Populate the store of a `TestNode` with entries and operations according to the passed config
/// and materialise the resulting documents. Additionally adds the relevant schema to the schema
/// provider.
///
/// Returns the key pairs of authors who published to the node and id's for all documents that were
/// materialised.
pub async fn populate_and_materialize(
    node: &mut TestNode,
    config: &PopulateStoreConfig,
) -> (Vec<KeyPair>, Vec<DocumentId>) {
    // Populate the store based with entries and operations based on the passed config.
    let (key_pairs, document_ids) = populate_store(&node.context.store, config).await;

    // Add the passed schema to the schema store.
    //
    // Note: The entries and operations which would normally exist for this schema will NOT be
    // present in the store, however the node will behave as expect as we directly inserted it into
    // the schema provider.
    let _ = node
        .context
        .schema_provider
        .update(config.schema.clone())
        .await;

    // Iterate over document id's and materialize into the store.
    for document_id in document_ids.clone() {
        // Create reduce task input.
        let input = TaskInput::DocumentId(document_id);
        // Run reduce task and collect returned dependency tasks.
        let next_tasks = reduce_task(node.context.clone(), input.clone())
            .await
            .expect("Reduce document");

        // Run dependency tasks.
        if let Some(tasks) = next_tasks {
            // We only want to issue dependency tasks.
            let dependency_tasks = tasks
                .iter()
                .filter(|task| task.worker_name() == "depenedency");

            for task in dependency_tasks {
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
///
/// Returns the document view id for the created document.
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

    let (entry_signed, _) = send_to_store(&node.context.store, &create_op, &schema, key_pair)
        .await
        .expect("Publish CREATE operation");

    let input = TaskInput::DocumentId(DocumentId::from(entry_signed.hash()));
    let next_tasks = reduce_task(node.context.clone(), input.clone())
        .await
        .expect("Reduce document");

    // Run dependency tasks
    if let Some(tasks) = next_tasks {
        // We only want to issue dependency tasks.
        let dependency_tasks = tasks
            .iter()
            .filter(|task| task.worker_name() == "depenedency");

        for task in dependency_tasks {
            dependency_task(node.context.clone(), task.input().to_owned())
                .await
                .expect("Run dependency task");
        }
    }
    DocumentViewId::from(entry_signed.hash())
}

/// Publish a schema, materialise it in a given `TestNode` and add it to the `SchemaProvider`.
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
            &node.context.store,
            &create_field_op,
            Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap(),
            key_pair,
        )
        .await
        .expect("Publish schema fields");

        let input = TaskInput::DocumentId(DocumentId::from(entry_signed.hash()));
        reduce_task(node.context.clone(), input).await.unwrap();

        info!("Added field '{}' ({})", field.0, field.1);
        field_ids.push(DocumentViewId::from(entry_signed.hash()));
    }

    // Build and reduce schema definition
    let create_schema_op = Schema::create(name, "test schema description", field_ids);
    let (entry_signed, _) = send_to_store(
        &node.context.store,
        &create_schema_op,
        Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
        key_pair,
    )
    .await
    .expect("Publish schema");

    let input = TaskInput::DocumentId(DocumentId::from(entry_signed.hash()));
    reduce_task(node.context.clone(), input.clone())
        .await
        .expect("Reduce schema document");

    // Run schema task for this spec
    let input = TaskInput::DocumentViewId(DocumentViewId::from(entry_signed.hash()));
    schema_task(node.context.clone(), input)
        .await
        .expect("Run schema task");

    let view_id = DocumentViewId::from(entry_signed.hash());
    let schema_id = SchemaId::Application(SchemaName::new(name).unwrap(), view_id);

    debug!("Done building {}", schema_id);
    node.context
        .schema_provider
        .get(&schema_id)
        .await
        .expect("Failed adding schema to provider.")
}

/// Create a schema in the test database and multiple documents using it.
pub async fn add_schema_and_documents(
    node: &mut TestNode,
    schema_name: &str,
    documents: Vec<Vec<(&str, OperationValue, Option<SchemaId>)>>,
    key_pair: &KeyPair,
) -> (Schema, Vec<DocumentViewId>) {
    assert!(documents.len() > 0);

    // Look at first document to automatically derive schema
    let schema_fields = documents[0]
        .iter()
        .map(|(field_name, field_value, schema_id)| {
            // Get field type from operation value
            let field_type = match field_value.field_type() {
                "relation" => FieldType::Relation(schema_id.clone().unwrap()),
                "pinned_relation" => FieldType::PinnedRelation(schema_id.clone().unwrap()),
                "relation_list" => FieldType::RelationList(schema_id.clone().unwrap()),
                "pinned_relation_list" => FieldType::PinnedRelationList(schema_id.clone().unwrap()),
                _ => field_value.field_type().parse().unwrap(),
            };

            (*field_name, field_type)
        })
        .collect();

    // Create schema
    let schema = add_schema(node, schema_name, schema_fields, key_pair).await;

    // Add all documents and return created view ids
    let mut view_ids = Vec::new();
    for document in documents {
        let fields = document
            .iter()
            .map(|field| (field.0, field.1.clone()))
            .collect();
        let view_id = add_document(node, schema.id(), fields, key_pair).await;
        view_ids.push(view_id);
    }

    (schema, view_ids)
}

/// Helper method for updating documents.
pub async fn update_document(
    node: &mut TestNode,
    schema_id: &SchemaId,
    fields: Vec<(&str, OperationValue)>,
    previous: &DocumentViewId,
    key_pair: &KeyPair,
) -> DocumentViewId {
    // Get requested schema from store.
    let schema = node
        .context
        .schema_provider
        .get(schema_id)
        .await
        .expect("Schema not found");

    // Build, publish and reduce an update operation for document.
    let create_op = OperationBuilder::new(schema.id())
        .action(OperationAction::Update)
        .fields(&fields)
        .previous(previous)
        .build()
        .expect("Build operation");

    let (entry_signed, _) = send_to_store(&node.context.store, &create_op, &schema, key_pair)
        .await
        .expect("Publish UPDATE operation");

    let document_id = node
        .context
        .store
        .get_document_id_by_operation_id(&OperationId::from(entry_signed.hash()))
        .await
        .expect("No db errors")
        .expect("Can get document id");

    let input = TaskInput::DocumentId(document_id);
    let next_tasks = reduce_task(node.context.clone(), input.clone())
        .await
        .expect("Reduce document");

    // Run dependency tasks
    if let Some(tasks) = next_tasks {
        // We only want to issue dependency tasks.
        let dependency_tasks = tasks
            .iter()
            .filter(|task| task.worker_name() == "dependency");

        for task in dependency_tasks {
            dependency_task(node.context.clone(), task.input().to_owned())
                .await
                .expect("Run dependency task");
        }
    }
    DocumentViewId::from(entry_signed.hash())
}

pub async fn add_blob(node: &mut TestNode, blob_data: &str, key_pair: &KeyPair) -> DocumentViewId {
    // Publish blob pieces and blob.
    let (blob_data_a, blob_data_b) = blob_data.split_at(blob_data.len() / 2);
    let blob_piece_view_id_1 = add_document(
        node,
        &SchemaId::BlobPiece(1),
        vec![("data", blob_data_a.into())],
        &key_pair,
    )
    .await;

    let blob_piece_view_id_2 = add_document(
        node,
        &SchemaId::BlobPiece(1),
        vec![("data", blob_data_b.into())],
        &key_pair,
    )
    .await;
    let blob_view_id = add_document(
        node,
        &SchemaId::Blob(1),
        vec![
            ("length", { blob_data.len() as i64 }.into()),
            ("mime_type", "text/plain".into()),
            (
                "pieces",
                vec![blob_piece_view_id_1, blob_piece_view_id_2].into(),
            ),
        ],
        &key_pair,
    )
    .await;

    blob_view_id
}

// Helper for asserting expected number of items yielded from a SQL query.
pub async fn assert_query(node: &TestNode, sql: &str, expected_len: usize) {
    let result: Result<Vec<String>, _> =
        query_scalar(sql).fetch_all(&node.context.store.pool).await;

    assert!(result.is_ok(), "{:#?}", result);
    assert_eq!(result.unwrap().len(), expected_len);
}
