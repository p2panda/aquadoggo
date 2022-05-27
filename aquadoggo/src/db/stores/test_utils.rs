// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{sign_and_encode, Entry};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::{
    Operation, OperationEncoded, OperationFields, OperationId, OperationValue, PinnedRelation,
    PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, LogStore, StorageProvider,
};
use p2panda_rs::storage_provider::traits::{EntryStore, OperationStore};
use p2panda_rs::test_utils::constants::{DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};

use crate::db::provider::SqlStorage;
use crate::graphql::client::EntryArgsRequest;
use crate::test_helpers::initialize_db;

use crate::db::stores::OperationStorage;
use crate::db::stores::StorageEntry;
use crate::db::stores::StorageLog;

pub fn test_create_operation() -> Operation {
    let mut fields = OperationFields::new();
    fields
        .add("username", OperationValue::Text("bubu".to_owned()))
        .unwrap();

    fields.add("height", OperationValue::Float(3.5)).unwrap();

    fields.add("age", OperationValue::Integer(28)).unwrap();

    fields
        .add("is_admin", OperationValue::Boolean(false))
        .unwrap();

    fields
        .add(
            "profile_picture",
            OperationValue::Relation(Relation::new(
                Hash::new("0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    .unwrap()
                    .into(),
            )),
        )
        .unwrap();
    fields
        .add(
            "special_profile_picture",
            OperationValue::PinnedRelation(PinnedRelation::new(
                Hash::new("0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .unwrap()
                    .into(),
            )),
        )
        .unwrap();
    fields
        .add(
            "many_profile_pictures",
            OperationValue::RelationList(RelationList::new(vec![
                Hash::new("0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .unwrap()
                    .into(),
                Hash::new("0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                    .unwrap()
                    .into(),
            ])),
        )
        .unwrap();
    fields
        .add(
            "many_special_profile_pictures",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
                    .unwrap()
                    .into(),
                Hash::new("0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                    .unwrap()
                    .into(),
            ])),
        )
        .unwrap();
    fields
        .add(
            "another_relation_field",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020abababababababababababababababababababababababababababababababab")
                    .unwrap()
                    .into(),
                Hash::new("0020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
                    .unwrap()
                    .into(),
            ])),
        )
        .unwrap();
    Operation::new_create(SchemaId::from_str(TEST_SCHEMA_ID).unwrap(), fields).unwrap()
}

pub fn test_update_operation(previous_operations: Vec<OperationId>, username: &str) -> Operation {
    let mut fields = OperationFields::new();
    fields
        .add("username", OperationValue::Text(username.to_owned()))
        .unwrap();

    Operation::new_update(
        SchemaId::from_str(TEST_SCHEMA_ID).unwrap(),
        previous_operations,
        fields,
    )
    .unwrap()
}

pub fn test_delete_operation(previous_operations: Vec<OperationId>) -> Operation {
    Operation::new_delete(
        SchemaId::from_str(TEST_SCHEMA_ID).unwrap(),
        previous_operations,
    )
    .unwrap()
}

pub fn test_key_pairs(no_of_authors: usize) -> Vec<KeyPair> {
    let mut key_pairs = vec![KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap()];

    for index in 1..no_of_authors {
        key_pairs.push(KeyPair::new())
    }

    key_pairs
}

/// Construct and return a storage provider instance backed by a pre-polpulated database. Passed parameters
/// define what the db should contain. The first entry in each log contains a valid CREATE operation
/// following entries contain duplicate UPDATE operations. If the with_delete flag is set to true
/// the last entry in all logs contain be a DELETE operation.
///
/// Returns a storage provider instance, a vector of key pairs for all authors in the db, and a vector
/// of the ids for all documents.
pub async fn test_db(
    // Number of entries per log/document
    no_of_entries: usize,
    // Number of authors, each with a log populated as defined above
    no_of_authors: usize,
    // A boolean flag for wether all logs should contain a delete operation
    with_delete: bool,
) -> (SqlStorage, Vec<KeyPair>, Vec<DocumentId>) {
    let pool = initialize_db().await;
    let storage_provider = SqlStorage { pool };

    // If we don't want any entries in the db return now
    if no_of_entries == 0 {
        return (storage_provider, Vec::default(), Vec::default());
    }

    let mut documents: Vec<DocumentId> = Vec::new();
    let schema = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();
    let key_pairs = test_key_pairs(no_of_authors);

    for key_pair in &key_pairs {
        let mut document: Option<DocumentId> = None;
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        for index in 0..no_of_entries {
            let next_entry_args = storage_provider
                .get_entry_args(&EntryArgsRequest {
                    author: author.clone(),
                    document: document.as_ref().cloned(),
                })
                .await
                .unwrap();

            let next_operation = if index == 0 {
                test_create_operation()
            } else if index == (no_of_entries - 1) && with_delete {
                test_delete_operation(vec![next_entry_args.backlink.clone().unwrap().into()])
            } else {
                test_update_operation(
                    vec![next_entry_args.backlink.clone().unwrap().into()],
                    "yoyo",
                )
            };

            let next_entry = Entry::new(
                &next_entry_args.log_id,
                Some(&next_operation),
                next_entry_args.backlink.as_ref(),
                next_entry_args.backlink.as_ref(),
                &next_entry_args.seq_num,
            )
            .unwrap();

            let entry_encoded = sign_and_encode(&next_entry, &key_pair).unwrap();
            let operation_encoded = OperationEncoded::try_from(&next_operation).unwrap();

            if index == 0 {
                document = Some(entry_encoded.hash().into());
                documents.push(entry_encoded.hash().into());
            }

            let storage_entry = StorageEntry::new(&entry_encoded, &operation_encoded).unwrap();

            storage_provider.insert_entry(storage_entry).await.unwrap();

            let storage_log = StorageLog::new(
                &author,
                &schema,
                &document.clone().unwrap(),
                &next_entry_args.log_id,
            );

            if next_entry_args.seq_num.is_first() {
                storage_provider.insert_log(storage_log).await.unwrap();
            }

            let storage_operation = OperationStorage::new(
                &author,
                &next_operation,
                &entry_encoded.hash().into(),
                &document.as_ref().cloned().unwrap(),
            );

            storage_provider
                .insert_operation(&storage_operation)
                .await
                .unwrap();
        }
    }
    (storage_provider, key_pairs, documents)
}
