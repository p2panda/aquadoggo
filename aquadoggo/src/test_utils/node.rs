// SPDX-License-Identifier: AGPL-3.0-or-later

use log::{debug, info};
use p2panda_rs::api::helpers::get_skiplink_for_entry;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{Document, DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::{encode_entry, sign_entry};
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::{OperationAction, OperationBuilder, OperationId, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId, SchemaName};
use p2panda_rs::storage_provider::traits::{EntryStore, LogStore, OperationStore};
use p2panda_rs::test_utils::memory_store::helpers::send_to_store;
use p2panda_rs::test_utils::memory_store::PublishedOperation;
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

/// Configuration used when populating the store for testing.
#[derive(Debug)]
pub struct PopulateStoreConfig {
    /// Number of entries per log/document.
    pub no_of_entries: usize,

    /// Number of logs for each public key.
    pub no_of_logs: usize,

    /// Number of public keys, each with logs populated as defined above.
    pub authors: Vec<KeyPair>,

    /// A boolean flag for wether all logs should contain a delete operation.
    pub with_delete: bool,

    /// The schema used for all operations in the db.
    pub schema: Schema,

    /// The fields used for every CREATE operation.
    pub create_operation_fields: Vec<(&'static str, OperationValue)>,

    /// The fields used for every UPDATE operation.
    pub update_operation_fields: Vec<(&'static str, OperationValue)>,
}

impl Default for PopulateStoreConfig {
    fn default() -> Self {
        Self {
            no_of_entries: 0,
            no_of_logs: 0,
            authors: vec![],
            with_delete: false,
            schema: doggo_schema(),
            create_operation_fields: doggo_fields(),
            update_operation_fields: doggo_fields(),
        }
    }
}

/// Fixture for constructing a `PopulateStoreConfig` with default values for aquadoggo tests.
///
/// Is passed to `p2panda_rs::test_utils::populate_store` or
/// `crate::test_utils::populate_and_materialize_unchecked` to populate a store or node with the specified
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

    // Key pairs used in data generation
    #[default(vec![])] authors: Vec<KeyPair>,

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
        authors,
        with_delete,
        schema,
        create_operation_fields,
        update_operation_fields,
    }
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
            .filter(|task| task.worker_name() == "dependency");

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

/// Splits bytes into chunks with a defined maximum length (256 bytes is the specified maximum) and
/// publishes a blob_piece_v1 document for each chunk.
pub async fn add_blob_pieces(
    node: &mut TestNode,
    body: &[u8],
    max_piece_length: usize,
    key_pair: &KeyPair,
) -> Vec<DocumentViewId> {
    let blob_pieces = body.chunks(max_piece_length);

    let mut blob_pieces_view_ids = Vec::with_capacity(blob_pieces.len());
    for piece in blob_pieces {
        let view_id = add_document(
            node,
            &SchemaId::BlobPiece(1),
            vec![("data", piece.into())],
            &key_pair,
        )
        .await;

        blob_pieces_view_ids.push(view_id);
    }

    blob_pieces_view_ids
}

pub async fn add_blob(
    node: &mut TestNode,
    body: &[u8],
    max_piece_length: usize,
    mime_type: &str,
    key_pair: &KeyPair,
) -> DocumentViewId {
    let blob_pieces_view_ids = add_blob_pieces(node, body, max_piece_length, key_pair).await;

    let blob_view_id = add_document(
        node,
        &SchemaId::Blob(1),
        vec![
            ("length", { body.len() as i64 }.into()),
            ("mime_type", mime_type.into()),
            ("pieces", blob_pieces_view_ids.into()),
        ],
        &key_pair,
    )
    .await;

    blob_view_id
}

pub async fn update_blob(
    node: &mut TestNode,
    body: &[u8],
    max_piece_length: usize,
    previous: &DocumentViewId,
    key_pair: &KeyPair,
) -> DocumentViewId {
    let blob_pieces_view_ids = add_blob_pieces(node, body, max_piece_length, key_pair).await;

    let blob_view_id = update_document(
        node,
        &SchemaId::Blob(1),
        vec![
            ("length", { body.len() as i64 }.into()),
            ("pieces", blob_pieces_view_ids.into()),
        ],
        &previous,
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
    assert_eq!(result.unwrap().len(), expected_len, "{:?}", sql);
}

/// Helper method for populating the store with test data.
///
/// Passed parameters define what the store should contain. The first entry in each log contains a
/// valid CREATE operation following entries contain UPDATE operations. If the with_delete flag is set
/// to true the last entry in all logs contain be a DELETE operation.
pub async fn populate_store_unchecked(
    store: &SqlStore,
    config: &PopulateStoreConfig,
) -> Vec<Document> {
    let mut documents: Vec<Document> = Vec::new();
    for key_pair in &config.authors {
        for log_id in 0..config.no_of_logs {
            let log_id = LogId::new(log_id as u64);
            let mut backlink: Option<Hash> = None;
            let mut previous: Option<DocumentViewId> = None;
            let mut current_document = None::<Document>;

            for seq_num in 1..config.no_of_entries + 1 {
                // Create an operation based on the current seq_num and whether this document should
                // contain a DELETE operation
                let operation = match seq_num {
                    // First operation is CREATE
                    1 => OperationBuilder::new(config.schema.id())
                        .fields(&config.create_operation_fields)
                        .build()
                        .expect("Error building operation"),
                    // Last operation is DELETE if the with_delete flag is set
                    seq if seq == (config.no_of_entries) && config.with_delete => {
                        OperationBuilder::new(config.schema.id())
                            .action(OperationAction::Delete)
                            .previous(&previous.expect("Previous should be set"))
                            .build()
                            .expect("Error building operation")
                    }
                    // All other operations are UPDATE
                    _ => OperationBuilder::new(config.schema.id())
                        .action(OperationAction::Update)
                        .fields(&config.update_operation_fields)
                        .previous(&previous.expect("Previous should be set"))
                        .build()
                        .expect("Error building operation"),
                };

                // Encode the operation.
                let encoded_operation =
                    encode_operation(&operation).expect("Failed encoding operation");

                // We need to calculate the skiplink.
                let seq_num = SeqNum::new(seq_num as u64).unwrap();
                let skiplink =
                    get_skiplink_for_entry(store, &seq_num, &log_id, &key_pair.public_key())
                        .await
                        .expect("Failed to get skiplink entry");

                // Construct and sign the entry.
                let entry = sign_entry(
                    &log_id,
                    &seq_num,
                    skiplink.as_ref(),
                    backlink.as_ref(),
                    &encoded_operation,
                    key_pair,
                )
                .expect("Failed signing entry");

                // Encode the entry.
                let encoded_entry = encode_entry(&entry).expect("Failed encoding entry");

                // Retrieve or derive the current document id.
                let document_id = match current_document.as_ref() {
                    Some(document) => document.id().to_owned(),
                    None => encoded_entry.hash().into(),
                };

                // Now we insert values into the database.

                // If the entries' seq num is 1 we insert a new log here.
                if entry.seq_num().is_first() {
                    store
                        .insert_log(
                            entry.log_id(),
                            entry.public_key(),
                            &config.schema.id(),
                            &document_id,
                        )
                        .await
                        .expect("Failed inserting log into store");
                }

                // Insert the entry into the store.
                store
                    .insert_entry(&entry, &encoded_entry, Some(&encoded_operation))
                    .await
                    .expect("Failed inserting entry into store");

                // Insert the operation into the store.
                store
                    .insert_operation(
                        &encoded_entry.hash().into(),
                        entry.public_key(),
                        &operation,
                        &document_id,
                    )
                    .await
                    .expect("Failed inserting operation into store");

                // Update the operations sorted index.
                store
                    .update_operation_index(
                        &encoded_entry.hash().into(),
                        seq_num.as_u64() as i32 - 1,
                    )
                    .await
                    .expect("Failed updating operation index");

                // Now we commit any changes to the document we are creating.
                let published_operation = PublishedOperation(
                    encoded_entry.hash().into(),
                    operation,
                    key_pair.public_key(),
                    document_id,
                );

                // Conditionally create or update the document.
                if let Some(mut document) = current_document {
                    document
                        .commit(&published_operation)
                        .expect("Failed updating document");
                    current_document = Some(document);
                } else {
                    current_document = Some(
                        DocumentBuilder::from(&vec![published_operation])
                            .build()
                            .expect("Failed to build document"),
                    );
                }

                // Set values used in the next iteration.
                backlink = Some(encoded_entry.hash());
                previous = Some(DocumentViewId::from(encoded_entry.hash()));
            }
            // Push the final document to the documents vec.
            documents.push(current_document.unwrap());
        }
    }
    documents
}

/// Populate the store of a `TestNode` with entries and operations according to the passed config
/// and materialise the resulting documents. Additionally adds the relevant schema to the schema
/// provider.
///
/// Returns the documents that were materialised.
pub async fn populate_and_materialize_unchecked(
    node: &mut TestNode,
    config: &PopulateStoreConfig,
) -> Vec<Document> {
    // Populate the store based with entries and operations based on the passed config.
    let documents = populate_store_unchecked(&node.context.store, config).await;

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

    // Iterate over documents and insert to the store.
    for document in documents.iter() {
        node.context
            .store
            .insert_document(document)
            .await
            .expect("Failed inserting document");
    }
    documents
}
