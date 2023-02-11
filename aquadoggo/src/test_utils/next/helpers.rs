// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{Document, DocumentId};
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::{
    OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::test_utils::constants;
use p2panda_rs::test_utils::fixtures::{schema, schema_fields};

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
