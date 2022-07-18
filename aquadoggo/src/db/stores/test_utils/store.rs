// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::{
    AsVerifiedOperation, Operation, OperationEncoded, OperationValue, VerifiedOperation,
};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::AsStorageEntry;
use p2panda_rs::test_utils::constants::SCHEMA_ID;
use p2panda_rs::test_utils::fixtures::{operation, operation_fields};

use crate::db::provider::SqlStorage;
use crate::db::stores::test_utils::{doggo_test_fields, test_key_pairs};
use crate::db::stores::StorageEntry;
use crate::domain::{next_args, publish};
use crate::graphql::client::NextEntryArguments;

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase {
    pub store: SqlStorage,
    pub test_data: TestData,
}

/// Data collected when populating a `TestDatabase` in order to easily check values which
/// would be otherwise hard or impossible to get through the store methods.
#[derive(Default)]
pub struct TestData {
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}

/// Configuration used when populating a `TestDatabase`.
pub struct PopulateDatabaseConfig {
    /// Number of entries per log/document.
    pub no_of_entries: usize,

    /// Number of logs for each author.
    pub no_of_logs: usize,

    /// Number of authors, each with logs populated as defined above.
    pub no_of_authors: usize,

    /// A boolean flag for wether all logs should contain a delete operation.
    pub with_delete: bool,

    /// The schema used for all operations in the db.
    pub schema: SchemaId,

    /// The fields used for every CREATE operation.
    pub create_operation_fields: Vec<(&'static str, OperationValue)>,

    /// The fields used for every UPDATE operation.
    pub update_operation_fields: Vec<(&'static str, OperationValue)>,
}

impl Default for PopulateDatabaseConfig {
    fn default() -> Self {
        Self {
            no_of_entries: 0,
            no_of_logs: 0,
            no_of_authors: 0,
            with_delete: false,
            schema: SCHEMA_ID.parse().unwrap(),
            create_operation_fields: doggo_test_fields(),
            update_operation_fields: doggo_test_fields(),
        }
    }
}

/// Helper method for populating a `TestDatabase` with configurable data.
///
/// Passed parameters define what the db should contain. The first entry in each log contains a
/// valid CREATE operation following entries contain duplicate UPDATE operations. If the
/// with_delete flag is set to true the last entry in all logs contain be a DELETE operation.
pub async fn populate_test_db(db: &mut TestDatabase, config: &PopulateDatabaseConfig) {
    let key_pairs = test_key_pairs(config.no_of_authors);

    for key_pair in &key_pairs {
        db.test_data
            .key_pairs
            .push(KeyPair::from_private_key(key_pair.private_key()).unwrap());

        for _log_id in 0..config.no_of_logs {
            let mut document_id: Option<DocumentId> = None;
            let mut previous_operation: Option<DocumentViewId> = None;

            for index in 0..config.no_of_entries {
                // Create an operation based on the current index and whether this document should
                // contain a DELETE operation
                let next_operation_fields = match index {
                    // First operation is CREATE
                    0 => Some(operation_fields(config.create_operation_fields.clone())),
                    // Last operation is DELETE if the with_delete flag is set
                    seq if seq == (config.no_of_entries - 1) && config.with_delete => None,
                    // All other operations are UPDATE
                    _ => Some(operation_fields(config.update_operation_fields.clone())),
                };

                // Publish the operation encoded on an entry to storage.
                let (entry_encoded, publish_entry_response) = send_to_store(
                    &db.store,
                    &operation(
                        next_operation_fields,
                        previous_operation,
                        Some(config.schema.to_owned()),
                    ),
                    document_id.as_ref(),
                    key_pair,
                )
                .await;

                // Set the previous_operations based on the backlink
                previous_operation = publish_entry_response.backlink.map(DocumentViewId::from);

                // If this was the first entry in the document, store the doucment id for later.
                if index == 0 {
                    document_id = Some(entry_encoded.hash().into());
                    db.test_data.documents.push(document_id.clone().unwrap());
                }
            }
        }
    }
}

/// Helper method for publishing an operation encoded on an entry to a store.
pub async fn send_to_store(
    store: &SqlStorage,
    operation: &Operation,
    document_id: Option<&DocumentId>,
    key_pair: &KeyPair,
) -> (EntrySigned, NextEntryArguments) {
    // Get an Author from the key_pair.
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
    let document_view_id: Option<DocumentViewId> =
        document_id.map(|id| id.as_str().parse().unwrap());

    let next_entry_args = next_args(store, &author, document_view_id.as_ref())
        .await
        .unwrap();

    // Construct the next entry.
    let next_entry = Entry::new(
        &next_entry_args.log_id.into(),
        Some(operation),
        next_entry_args.skiplink.map(Hash::from).as_ref(),
        next_entry_args.backlink.map(Hash::from).as_ref(),
        &next_entry_args.seq_num.into(),
    )
    .unwrap();

    // Encode both the entry and operation.
    let entry_encoded = sign_and_encode(&next_entry, key_pair).unwrap();
    let operation_encoded = OperationEncoded::try_from(operation).unwrap();

    // Construct a the required types for publishing an entry and operation.
    let entry = StorageEntry::new(&entry_encoded, &operation_encoded).unwrap();
    let operation = VerifiedOperation::new_from_entry(&entry_encoded, &operation_encoded).unwrap();

    // Publish the entry and get the next entry args.
    let publish_entry_response = publish(store, &entry, &operation).await.unwrap();

    (entry_encoded, publish_entry_response)
}
