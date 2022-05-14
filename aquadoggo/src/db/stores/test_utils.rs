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
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::storage_provider::traits::StorageProvider;
use p2panda_rs::test_utils::constants::{DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};

use crate::db::provider::SqlStorage;
use crate::graphql::client::{EntryArgsRequest, PublishEntryRequest};
use crate::test_helpers::initialize_db;

use super::operation::OperationStorage;

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

pub async fn test_db(no_of_entries: usize, with_delete: bool) -> SqlStorage {
    let pool = initialize_db().await;
    let storage_provider = SqlStorage { pool };

    // If we don't want any entries in the db return now
    if no_of_entries == 0 {
        return storage_provider;
    }

    let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

    let mut document: Option<DocumentId> = None;

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
        } else if index == no_of_entries && with_delete {
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
            next_entry_args.skiplink.as_ref(),
            next_entry_args.backlink.as_ref(),
            &next_entry_args.seq_num,
        )
        .unwrap();

        let entry_encoded = sign_and_encode(&next_entry, &key_pair).unwrap();
        let operation_encoded = OperationEncoded::try_from(&next_operation).unwrap();

        if index == 0 {
            document = Some(entry_encoded.hash().into());
        }

        let operation_id = entry_encoded.hash().into();

        // Publish the new entry
        storage_provider
            .publish_entry(&PublishEntryRequest {
                entry_encoded: entry_encoded.clone(),
                operation_encoded,
            })
            .await
            .unwrap();

        let storage_operation = OperationStorage::new(
            &author,
            &next_operation,
            &operation_id,
            &document.as_ref().cloned().unwrap(),
        );

        // Publish the new operation
        storage_provider
            .insert_operation(&storage_operation)
            .await
            .unwrap();
    }
    storage_provider
}
