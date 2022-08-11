// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::{EncodedEntry, Entry};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::traits::{AsOperation, AsVerifiedOperation};
use p2panda_rs::operation::{
    EncodedOperation, Operation, OperationValue, PinnedRelation, PinnedRelationList, Relation,
    RelationList,
};

use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};
use p2panda_rs::test_utils::constants::PRIVATE_KEY;

use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;
use crate::domain::{next_args, publish};

/// A complex set of fields which can be used in aquadoggo tests.
pub fn doggo_test_fields() -> Vec<(&'static str, OperationValue)> {
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

/// Helper for creating many key_pairs.
///
/// The first keypair created will allways be `PRIVATE_KEY`.
pub fn test_key_pairs(no_of_authors: usize) -> Vec<KeyPair> {
    let mut key_pairs = vec![KeyPair::from_private_key_str(PRIVATE_KEY).unwrap()];

    for _index in 1..no_of_authors {
        key_pairs.push(KeyPair::new())
    }

    key_pairs
}

/// Helper for constructing a valid encoded entry and operation using valid next_args retrieved
/// from the passed store.
pub async fn encode_entry_and_operation<S: StorageProvider>(
    store: &S,
    operation: &Operation,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
) -> (EncodedEntry, EncodedOperation) {
    let author = Author::from(key_pair.public_key());
    let document_view_id: Option<DocumentViewId> =
        document_id.map(|id| id.as_str().parse().unwrap());

    // Get next args
    let next_args = next_args::<S>(store, &author, document_view_id.as_ref())
        .await
        .unwrap();

    // Sign and encode the entry and operation.
    let operation_encoded = encode_operation(operation).unwrap();
    let entry_encoded = sign_and_encode_entry(
        &next_args.log_id.into(),
        &next_args.seq_num.into(),
        next_args.skiplink.map(Hash::from).as_ref(),
        next_args.backlink.map(Hash::from).as_ref(),
        &operation_encoded,
        key_pair,
    )
    .unwrap();

    // Return encoded entry and operation.
    (entry_encoded, operation_encoded)
}

/// Helper for inserting an entry, operation and document_view into the store.
pub async fn insert_entry_operation_and_view(
    store: &SqlStorage,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
    operation: &Operation,
) -> (DocumentId, DocumentViewId) {
    if !operation.is_create() && document_id.is_none() {
        panic!("UPDATE and DELETE operations require a DocumentId to be passed")
    }

    // Encode entry and operation.
    let (entry_signed, operation_encoded) =
        encode_entry_and_operation(store, operation, key_pair, document_id).await;

    // Unwrap document_id or construct it from the entry hash.
    let document_id = document_id
        .cloned()
        .unwrap_or_else(|| entry_signed.hash().into());
    let document_view_id: DocumentViewId = entry_signed.hash().into();

    // Publish the entry.
    publish(store, &entry_signed, &operation_encoded)
        .await
        .unwrap();

    // Materialise the effected document.
    let document_operations = store
        .get_operations_by_document_id(&document_id)
        .await
        .unwrap();
    let document = DocumentBuilder::new(document_operations).build().unwrap();
    store.insert_document(&document).await.unwrap();

    // Return the document_id and document_view_id.
    (document_id, document_view_id)
}
