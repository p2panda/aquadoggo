// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{Document, DocumentId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::{
    OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::{Schema, SchemaId, SchemaName};
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::test_utils::constants;
use p2panda_rs::test_utils::fixtures::{random_document_view_id, schema, schema_fields};
use rstest::fixture;

use crate::replication::SchemaIdSet;

/// Schema id used in aquadoggo tests.
fn doggo_schema_id() -> SchemaId {
    SchemaId::new_application(
        &SchemaName::new("doggo_schema").unwrap(),
        &constants::HASH.to_owned().parse().unwrap(),
    )
}

/// Schema used in aquadoggo tests.
pub fn doggo_schema() -> Schema {
    schema(
        schema_fields(doggo_fields(), doggo_schema_id()),
        doggo_schema_id(),
        "A doggo schema for testing",
    )
}

/// A complex set of fields which are used in aquadoggo tests.
pub fn doggo_fields() -> Vec<(&'static str, OperationValue)> {
    vec![
        ("username", OperationValue::String("bubu".to_owned())),
        ("data", OperationValue::Bytes(vec![0, 1, 2, 3])),
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
        (
            "an_empty_relation_list",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![])),
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

/// Helper for constructing a schema from a vec of field values.
pub fn schema_from_fields(fields: Vec<(&str, OperationValue)>) -> Schema {
    schema(
        schema_fields(fields, constants::SCHEMA_ID.parse().unwrap()),
        constants::SCHEMA_ID.parse().unwrap(),
        "A doggo schema for testing",
    )
}

#[fixture]
pub fn random_schema_id_set() -> SchemaIdSet {
    let system_schema_id = SchemaId::SchemaFieldDefinition(1);
    let document_view_id_1 = random_document_view_id();
    let schema_id_1 =
        SchemaId::new_application(&SchemaName::new("messages").unwrap(), &document_view_id_1);
    let document_view_id_2 = random_document_view_id();
    let schema_id_2 =
        SchemaId::new_application(&SchemaName::new("events").unwrap(), &document_view_id_2);
    SchemaIdSet::new(&[system_schema_id, schema_id_1, schema_id_2])
}

pub fn generate_key_pairs(num: u64) -> Vec<KeyPair> {
    (0..num).map(|_| KeyPair::new()).collect()
}
