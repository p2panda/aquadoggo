// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::{sign_and_encode, Entry, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::{
    Operation, OperationEncoded, OperationFields, OperationValue, PinnedRelation,
    PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::StorageProvider;
use p2panda_rs::test_utils::constants::{DEFAULT_HASH, DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};

use crate::db::sql_store::SqlStorage;
use crate::rpc::{EntryArgsRequest, PublishEntryRequest};
use crate::test_helpers::initialize_db;

pub fn test_operation() -> Operation {
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
            OperationValue::Relation(Relation::new(DEFAULT_HASH.parse().unwrap())),
        )
        .unwrap();
    fields
        .add(
            "special_profile_picture",
            OperationValue::PinnedRelation(PinnedRelation::new(DEFAULT_HASH.parse().unwrap())),
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
                Hash::new("0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                    .unwrap()
                    .into(),
                Hash::new("0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .unwrap()
                    .into(),
            ])),
        )
        .unwrap();
    Operation::new_create(SchemaId::from_str(TEST_SCHEMA_ID).unwrap(), fields).unwrap()
}

pub async fn test_db() -> SqlStorage {
    let pool = initialize_db().await;
    let storage_provider = SqlStorage { pool };

    let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
    let schema = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();
    let create_operation = test_operation();
    let create_entry = Entry::new(
        &LogId::default(),
        Some(&create_operation),
        None,
        None,
        &SeqNum::new(1).unwrap(),
    )
    .unwrap();

    let entry_encoded = sign_and_encode(&create_entry, &key_pair).unwrap();
    let operation_encoded = OperationEncoded::try_from(&create_operation).unwrap();

    let document: DocumentId = entry_encoded.hash().into();

    storage_provider
        .publish_entry(&PublishEntryRequest {
            entry_encoded,
            operation_encoded,
        })
        .await
        .unwrap();

    let mut fields = OperationFields::new();
    fields
        .add("username", OperationValue::Text("yahoooo".to_owned()))
        .unwrap();

    for _ in 1..100 {
        let next_entry_args = storage_provider
            .get_entry_args(&EntryArgsRequest {
                author: author.clone(),
                document: Some(document.clone()),
            })
            .await
            .unwrap();

        let backlink = next_entry_args.entry_hash_backlink.clone().unwrap();

        let update_operation =
            Operation::new_update(schema.clone(), vec![backlink.into()], fields.clone()).unwrap();

        let update_entry = Entry::new(
            &next_entry_args.log_id,
            Some(&update_operation),
            next_entry_args.entry_hash_skiplink.as_ref(),
            next_entry_args.entry_hash_backlink.as_ref(),
            &next_entry_args.seq_num,
        )
        .unwrap();

        let entry_encoded = sign_and_encode(&update_entry, &key_pair).unwrap();
        let operation_encoded = OperationEncoded::try_from(&update_operation).unwrap();

        storage_provider
            .publish_entry(&PublishEntryRequest {
                entry_encoded,
                operation_encoded,
            })
            .await
            .unwrap();
    }
    storage_provider
}
