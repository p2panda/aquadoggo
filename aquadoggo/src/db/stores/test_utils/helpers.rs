// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
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
    // TODO: Need full refactor
    todo!()
}
