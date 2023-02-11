// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use log::{debug, info};
use p2panda_rs::document::{Document, DocumentId, DocumentViewId};
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{
    OperationBuilder, OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::test_utils::constants;
use p2panda_rs::test_utils::fixtures::{schema, schema_fields};
use p2panda_rs::test_utils::memory_store::helpers::send_to_store;

use crate::test_utils::TestDatabase;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::TaskInput;

fn doggo_schema_id() -> SchemaId {
    SchemaId::new_application("doggo_schema", &constants::HASH.to_owned().parse().unwrap())
}

pub fn doggo_schema() -> Schema {
    schema(
        schema_fields(doggo_fields(), doggo_schema_id()),
        doggo_schema_id(),
        "A doggo schema for testing",
    )
}

/// A complex set of fields which can be used in aquadoggo tests.
pub fn doggo_fields() -> Vec<(&'static str, OperationValue)> {
    vec![
        ("username", OperationValue::String("bubu".to_owned())),
        ("height", OperationValue::Float(3.5)),
        ("age", OperationValue::Integer(28)),
        ("is_admin", OperationValue::Boolean(false)),
        (
            "profile_picture",
            OperationValue::Relation(Relation::new(
                Hash::new("0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    .unwrap()
                    .into(),
            )),
        ),
        (
            "special_profile_picture",
            OperationValue::PinnedRelation(PinnedRelation::new(
                Hash::new("0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .unwrap()
                    .into(),
            )),
        ),
        (
            "many_profile_pictures",
            OperationValue::RelationList(RelationList::new(vec![
                Hash::new("0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .unwrap()
                    .into(),
                Hash::new("0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                    .unwrap()
                    .into(),
            ])),
        ),
        (
            "many_special_profile_pictures",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
                    .unwrap()
                    .into(),
                Hash::new("0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                    .unwrap()
                    .into(),
            ])),
        ),
        (
            "another_relation_field",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020abababababababababababababababababababababababababababababababab")
                    .unwrap()
                    .into(),
                Hash::new("0020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
                    .unwrap()
                    .into(),
            ])),
        ),
    ]
}

/// Build a document from it's stored operations specified by it's document id.
pub async fn build_document<S: OperationStore>(store: &S, document_id: &DocumentId) -> Document {
    // We retrieve the operations.
    let document_operations = store
        .get_operations_by_document_id(document_id)
        .await
        .expect("Get operations");

    // Then we construct the document.
    Document::try_from(&document_operations).expect("Build the document")
}

/// Publish a document and materialise it in a given `TestDatabase`.
///
/// Also runs dependency task for document.
pub async fn add_document(
    test_db: &mut TestDatabase,
    schema_id: &SchemaId,
    fields: Vec<(&str, OperationValue)>,
    key_pair: &KeyPair,
) -> DocumentViewId {
    info!("Creating document for {}", schema_id);

    // Get requested schema from store.
    let schema = test_db
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

    let (entry_signed, _) = send_to_store(&test_db.store, &create_op, &schema, key_pair)
        .await
        .expect("Publish CREATE operation");

    let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
    let dependency_tasks = reduce_task(test_db.context.clone(), input.clone())
        .await
        .expect("Reduce document");

    // Run dependency tasks
    if let Some(tasks) = dependency_tasks {
        for task in tasks {
            dependency_task(test_db.context.clone(), task.input().to_owned())
                .await
                .expect("Run dependency task");
        }
    }
    DocumentViewId::from(entry_signed.hash())
}

/// Publish a schema and materialise it in a given `TestDatabase`.
pub async fn add_schema(
    test_db: &mut TestDatabase,
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
            &test_db.store,
            &create_field_op,
            Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap(),
            key_pair,
        )
        .await
        .expect("Publish schema fields");

        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        reduce_task(test_db.context.clone(), input).await.unwrap();

        info!("Added field '{}' ({})", field.0, field.1);
        field_ids.push(DocumentViewId::from(entry_signed.hash()));
    }

    // Build and reduce schema definition
    let create_schema_op = Schema::create(name, "test schema description", field_ids);
    let (entry_signed, _) = send_to_store(
        &test_db.store,
        &create_schema_op,
        Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
        key_pair,
    )
    .await
    .expect("Publish schema");

    let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
    reduce_task(test_db.context.clone(), input.clone())
        .await
        .expect("Reduce schema document");

    // Run schema task for this spec
    let input = TaskInput::new(None, Some(DocumentViewId::from(entry_signed.hash())));
    schema_task(test_db.context.clone(), input)
        .await
        .expect("Run schema task");

    let view_id = DocumentViewId::from(entry_signed.hash());
    let schema_id = SchemaId::Application(name.to_string(), view_id);

    debug!("Done building {}", schema_id);
    test_db
        .context
        .schema_provider
        .get(&schema_id)
        .await
        .expect("Failed adding schema to provider.")
}
