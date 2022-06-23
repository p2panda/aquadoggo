// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::entry::{sign_and_encode, Entry};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::{
    AsOperation, Operation, OperationEncoded, OperationId, OperationValue, PinnedRelation,
    PinnedRelationList, Relation, RelationList, VerifiedOperation,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, EntryStore, LogStore, OperationStore, StorageProvider,
};
use p2panda_rs::test_utils::constants::{DEFAULT_PRIVATE_KEY, TEST_SCHEMA_ID};
use p2panda_rs::test_utils::fixtures::{create_operation, delete_operation, update_operation};
use rstest::fixture;

use crate::db::provider::SqlStorage;
use crate::db::stores::{StorageEntry, StorageLog};
use crate::db::traits::DocumentStore;
use crate::graphql::client::{EntryArgsRequest, PublishEntryRequest};
use crate::test_helpers::initialize_db;

/// The fields used as defaults in the tests.
pub fn doggo_test_fields() -> Vec<(&'static str, OperationValue)> {
    vec![
        ("username", OperationValue::Text("bubu".to_owned())),
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
pub fn test_key_pairs(no_of_authors: usize) -> Vec<KeyPair> {
    let mut key_pairs = vec![KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap()];

    for _index in 1..no_of_authors {
        key_pairs.push(KeyPair::new())
    }

    key_pairs
}

/// Helper for constructing a publish entry request.
pub async fn construct_publish_entry_request(
    provider: &SqlStorage,
    operation: &Operation,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
) -> PublishEntryRequest {
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
    let entry_args_request = EntryArgsRequest {
        author: author.clone(),
        document: document_id.cloned(),
    };
    let next_entry_args = provider.get_entry_args(&entry_args_request).await.unwrap();

    let entry = Entry::new(
        &next_entry_args.log_id,
        Some(operation),
        next_entry_args.skiplink.as_ref(),
        next_entry_args.backlink.as_ref(),
        &next_entry_args.seq_num,
    )
    .unwrap();

    let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();
    let operation_encoded = OperationEncoded::try_from(operation).unwrap();
    PublishEntryRequest {
        entry_encoded,
        operation_encoded,
    }
}

/// Helper for inserting an entry, operation and document_view into the database.
pub async fn insert_entry_operation_and_view(
    provider: &SqlStorage,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
    operation: &Operation,
) -> (DocumentId, DocumentViewId) {
    if !operation.is_create() && document_id.is_none() {
        panic!("UPDATE and DELETE operations require a DocumentId to be passed")
    }

    let request = construct_publish_entry_request(provider, operation, key_pair, document_id).await;

    let operation_id: OperationId = request.entry_encoded.hash().into();
    let document_id = document_id
        .cloned()
        .unwrap_or_else(|| request.entry_encoded.hash().into());

    let document_view_id: DocumentViewId = request.entry_encoded.hash().into();

    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

    provider.publish_entry(&request).await.unwrap();
    provider
        .insert_operation(
            &VerifiedOperation::new(&author, &operation_id, operation).unwrap(),
            &document_id,
        )
        .await
        .unwrap();

    let document_operations = provider
        .get_operations_by_document_id(&document_id)
        .await
        .unwrap();

    let document = DocumentBuilder::new(document_operations).build().unwrap();

    provider.insert_document(&document).await.unwrap();

    (document_id, document_view_id)
}

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestSqlStore {
    pub store: SqlStorage,
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}

impl TestSqlStore {
    /// Close database connection.
    pub async fn close(&self) {
        self.store.pool.close().await;
    }
}

/// Fixture for constructing a storage provider instance backed by a pre-polpulated database. Passed
/// parameters define what the db should contain. The first entry in each log contains a valid CREATE
/// operation following entries contain duplicate UPDATE operations. If the with_delete flag is set
/// to true the last entry in all logs contain be a DELETE operation.
///
/// Returns a `TestSqlStore` containing storage provider instance, a vector of key pairs for all authors
/// in the db, and a vector of the ids for all documents.
#[fixture]
pub async fn test_db(
    // Number of entries per log/document
    #[default(0)] no_of_entries: usize,
    // Number of authors, each with a log populated as defined above
    #[default(0)] no_of_authors: usize,
    // A boolean flag for wether all logs should contain a delete operation
    #[default(false)] with_delete: bool,
    // The schema used for all operations in the db
    #[default(TEST_SCHEMA_ID.parse().unwrap())] schema: SchemaId,
    // The fields used for every CREATE operation
    #[default(doggo_test_fields())] create_operation_fields: Vec<(&'static str, OperationValue)>,
    // The fields used for every UPDATE operation
    #[default(doggo_test_fields())] update_operation_fields: Vec<(&'static str, OperationValue)>,
) -> TestSqlStore {
    let mut documents: Vec<DocumentId> = Vec::new();
    let key_pairs = test_key_pairs(no_of_authors);

    let pool = initialize_db().await;
    let store = SqlStorage::new(pool);

    // If we don't want any entries in the db return now
    if no_of_entries == 0 {
        return TestSqlStore {
            store,
            key_pairs,
            documents,
        };
    }

    for key_pair in &key_pairs {
        let mut document: Option<DocumentId> = None;
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        for index in 0..no_of_entries {
            let next_entry_args = store
                .get_entry_args(&EntryArgsRequest {
                    author: author.clone(),
                    document: document.as_ref().cloned(),
                })
                .await
                .unwrap();

            let next_operation = if index == 0 {
                create_operation(&create_operation_fields)
            } else if index == (no_of_entries - 1) && with_delete {
                delete_operation(&next_entry_args.backlink.clone().unwrap().into())
            } else {
                update_operation(
                    &update_operation_fields,
                    &next_entry_args.backlink.clone().unwrap().into(),
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

            let entry_encoded = sign_and_encode(&next_entry, key_pair).unwrap();
            let operation_encoded = OperationEncoded::try_from(&next_operation).unwrap();

            if index == 0 {
                document = Some(entry_encoded.hash().into());
                documents.push(entry_encoded.hash().into());
            }

            let storage_entry = StorageEntry::new(&entry_encoded, &operation_encoded).unwrap();

            store.insert_entry(storage_entry).await.unwrap();

            let storage_log = StorageLog::new(
                &author,
                &schema,
                &document.clone().unwrap(),
                &next_entry_args.log_id,
            );

            if next_entry_args.seq_num.is_first() {
                store.insert_log(storage_log).await.unwrap();
            }

            let verified_operation =
                VerifiedOperation::new(&author, &entry_encoded.hash().into(), &next_operation)
                    .unwrap();

            store
                .insert_operation(&verified_operation, &document.clone().unwrap())
                .await
                .unwrap();
        }
    }

    TestSqlStore {
        store,
        key_pairs,
        documents,
    }
}
