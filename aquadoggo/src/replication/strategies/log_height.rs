// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use log::trace;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::traits::{AsEncodedEntry, AsEntry};
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
use p2panda_rs::Human;

use crate::db::types::StorageEntry;
use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::diff_log_heights;
use crate::replication::traits::Strategy;
use crate::replication::{LogHeights, Message, Mode, SchemaIdSet, StrategyResult};
use crate::schema::SchemaProvider;

type SortedIndex = i32;

fn has_blob_relation(schema: &Schema) -> bool {
    for (_, field_type) in schema.fields().iter() {
        match field_type {
            p2panda_rs::schema::FieldType::Relation(schema_id)
            | p2panda_rs::schema::FieldType::RelationList(schema_id)
            | p2panda_rs::schema::FieldType::PinnedRelation(schema_id)
            | p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
                if schema_id == &SchemaId::Blob(1) {
                    return true;
                }
            }
            _ => (),
        }
    }
    false
}

/// Retrieve entries from the store, group the result by document id and then sub-order them by
/// their sorted index.
async fn retrieve_entries(
    store: &SqlStore,
    remote_needs: &[LogHeights],
) -> Vec<(StorageEntry, DocumentId, SortedIndex)> {
    let mut entries = Vec::new();

    for (public_key, log_heights) in remote_needs {
        for (log_id, seq_num) in log_heights {
            // Get the entries the remote needs for each log.
            let log_entries = store
                .get_entries_from(public_key, log_id, seq_num)
                .await
                .expect("Fatal database error");

            for entry in log_entries {
                // Get the entry as well as we need some additional information in order to
                // send the entries in the correct order.
                let operation = store
                    .get_operation(&entry.hash().into())
                    .await
                    .expect("Fatal database error")
                    .expect("Operation should be in store");

                // We only send entries if their operation has been materialized.
                if let Some(sorted_index) = operation.sorted_index {
                    entries.push((entry, operation.document_id, sorted_index));
                }
            }
        }
    }

    // Sort all entries by document_id & sorted_index.
    entries.sort_by(
        |(_, document_id_a, sorted_index_a), (_, document_id_b, sorted_index_b)| {
            (document_id_a, sorted_index_a).cmp(&(document_id_b, sorted_index_b))
        },
    );

    entries
}

#[derive(Clone, Debug)]
pub struct LogHeightStrategy {
    schema_provider: SchemaProvider,
    target_set: SchemaIdSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl LogHeightStrategy {
    pub fn new(target_set: &SchemaIdSet, schema_provider: SchemaProvider) -> Self {
        Self {
            schema_provider,
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }

    /// Calculate the documents which should be included in this replication session.
    ///
    /// This is based on the schema ids included in the target set and any document dependencies
    /// which we have on our local node. Documents which are of type `blob_v1` are only included
    /// if the `blob_v1` schema is included in the target set _and_ the blob document is related
    /// to from another document (also of a schema included in the target set). The same is true
    /// of `blob_piece_v1` documents. These are only included if a blob document is also included
    /// which relates to them.
    ///
    /// For example, a target set including the schema id `[img_0020, blob_v1]` would look at all
    /// `img_0020` documents and only include blobs which they relate to.
    async fn included_document_ids(&self, store: &SqlStore) -> Vec<DocumentId> {
        let wants_blobs = self.target_set().contains(&SchemaId::Blob(1));
        let wants_blob_pieces = self.target_set().contains(&SchemaId::BlobPiece(1));
        let mut all_target_documents = vec![];
        let mut all_blob_documents = vec![];
        let mut all_blob_piece_documents = vec![];
        for schema_id in self.target_set().iter() {
            // If the schema is `blob_v1` or `blob_piece_v1` we don't take any action and just
            // move onto the next loop as these types of documents are only included as part of
            // other application documents.
            if schema_id == &SchemaId::Blob(1) || schema_id == &SchemaId::BlobPiece(1) {
                continue;
            }

            // Check if documents of this type contain a relation to a blob document.
            let has_blob_relation = match self.schema_provider.get(schema_id).await {
                Some(schema) => has_blob_relation(&schema),
                None => false,
            };

            // Get the ids of all documents of this schema.
            let schema_documents: Vec<DocumentId> = store
                .get_documents_by_schema(schema_id)
                .await
                .unwrap()
                .iter()
                .map(|document| document.id())
                .cloned()
                .collect();

            let mut schema_blob_documents = vec![];

            // If the target set included `blob_v1` schema_id then we collect any related blob documents.
            if wants_blobs && has_blob_relation {
                for document_id in &schema_documents {
                    let blob_documents = store.get_blob_child_relations(document_id).await.unwrap();
                    schema_blob_documents.extend(blob_documents)
                }
            }

            // If `blob_piece_v1` is included in the target set.
            if wants_blob_pieces && has_blob_relation {
                for blob_id in &schema_blob_documents {
                    // Get all existing views for this blob document.
                    let blob_document_view_ids = store
                        .get_all_document_view_ids(blob_id)
                        .await
                        .expect("Fatal database error");
                    for blob_view_id in blob_document_view_ids {
                        // Get all pieces for each blob view.
                        let blob_piece_ids = store
                            .get_child_document_ids(&blob_view_id)
                            .await
                            .expect("Fatal database error");
                        all_blob_piece_documents.extend(blob_piece_ids)
                    }
                }
            }

            all_target_documents.extend(schema_documents);
            all_blob_documents.extend(schema_blob_documents);
        }

        let mut all_included_document_ids = vec![];
        all_included_document_ids.extend(all_target_documents);
        all_included_document_ids.extend(all_blob_documents);
        all_included_document_ids.extend(all_blob_piece_documents);

        all_included_document_ids
    }

    // Calculate the heights of all logs which contain contributions to documents in the current
    // `SchemaIdSet`.
    async fn local_log_heights(
        &self,
        store: &SqlStore,
        included_documents: &[DocumentId],
    ) -> HashMap<PublicKey, Vec<(LogId, SeqNum)>> {
        // For every included document calculate the heights of any contributing logs.
        store
            .get_document_log_heights(included_documents)
            .await
            .expect("Fatal database error")
            .into_iter()
            .collect()
    }

    // Prepare entry responses based on a remotes log heights. The response contains all entries
    // the remote needs to bring their state in line with our own. The returned entries are
    // grouped by document and ordered by the `sorted_index` of the operations they carry. With
    // this ordering they can be ingested and validated easily on the remote.
    async fn entry_responses(
        &self,
        store: &SqlStore,
        remote_log_heights: &[LogHeights],
    ) -> Vec<Message> {
        // Calculate which documents should be included in the log height.
        let included_document_ids = self.included_document_ids(store).await;

        // Get local log heights for the configured target set.
        let local_log_heights = self.local_log_heights(store, &included_document_ids).await;

        // Compare local and remote log heights to determine what they need from us.
        let remote_needs = diff_log_heights(
            &local_log_heights,
            &remote_log_heights.iter().cloned().collect(),
        );

        let entries = retrieve_entries(store, &remote_needs).await;

        // Compose the actual messages.
        entries
            .iter()
            .map(|(entry, _, _)| {
                trace!(
                    "Prepare message containing entry at {:?} on {:?} for {}",
                    entry.seq_num(),
                    entry.log_id(),
                    entry.public_key().display()
                );

                Message::Entry(entry.clone().encoded_entry, entry.payload().cloned())
            })
            .collect()
    }
}

#[async_trait]
impl Strategy for LogHeightStrategy {
    fn mode(&self) -> Mode {
        Mode::LogHeight
    }

    fn target_set(&self) -> SchemaIdSet {
        self.target_set.clone()
    }

    async fn initial_messages(&mut self, store: &SqlStore) -> StrategyResult {
        // Calculate which documents should be included in the log height.
        let included_document_ids = self.included_document_ids(store).await;

        let log_heights = self.local_log_heights(store, &included_document_ids).await;
        self.sent_have = true;

        StrategyResult {
            is_local_done: log_heights.is_empty(),
            messages: vec![Message::Have(log_heights.into_iter().collect())],
        }
    }

    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        let mut result = StrategyResult {
            is_local_done: false,
            messages: vec![],
        };

        // Send our Have message to remote if we haven't done it yet
        if !self.sent_have {
            result.merge(self.initial_messages(store).await);
        }

        match message {
            Message::Have(remote_log_heights) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                let response = self.entry_responses(store, remote_log_heights).await;
                result.messages.extend(response);
                result.is_local_done = true;

                self.received_remote_have = true;
            }
            _ => {
                return Err(ReplicationError::StrategyFailed(
                    "Received unknown message type".into(),
                ));
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::{EncodedEntry, LogId, SeqNum};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        EncodedOperation, OperationAction, OperationBuilder, OperationValue,
    };
    use p2panda_rs::schema::{Schema, SchemaId};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use p2panda_rs::test_utils::generate_random_bytes;
    use p2panda_rs::test_utils::memory_store::helpers::send_to_store;
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::replication::ingest::SyncIngest;
    use crate::replication::strategies::log_height::{retrieve_entries, SortedIndex};
    use crate::replication::{LogHeightStrategy, LogHeights, Message, SchemaIdSet};
    use crate::test_utils::{
        add_blob, add_schema_and_documents, generate_key_pairs, populate_and_materialize_unchecked,
        populate_store_config, test_runner_with_manager, PopulateStoreConfig, TestNode,
        TestNodeManager,
    };

    // Helper for retrieving operations ordered as expected for replication and testing the result.
    async fn retrieve_and_test_entries(
        node: &TestNode,
        remote_needs: &[LogHeights],
        expected_entries: &Vec<(DocumentId, SortedIndex)>,
    ) {
        // Retrieve the entries.
        let entries = retrieve_entries(&node.context.store, &remote_needs).await;

        // Map the returned value into a more easily testable form (we assume the entries are
        // correct, here we are testing the entry retrieval logic mainly)
        let entries: Vec<(DocumentId, SortedIndex)> = entries
            .into_iter()
            .map(|(_, document_id, sorted_index)| (document_id, sorted_index))
            .collect();

        assert_eq!(&entries, expected_entries, "{remote_needs:#?}");
    }

    // Helper for updating a document.
    async fn update_and_materialize_document(
        node: &TestNode,
        key_pair: &KeyPair,
        schema: &Schema,
        document_id: &DocumentId,
        values: Vec<(&str, impl Into<OperationValue>)>,
    ) {
        let values: Vec<(&str, OperationValue)> = values
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();

        // Publish an update into this documents past.
        let update_operation = OperationBuilder::new(schema.id())
            .action(OperationAction::Update)
            .previous(&document_id.as_str().parse().unwrap())
            .fields(&values)
            .build()
            .unwrap();

        send_to_store(&node.context.store, &update_operation, schema, key_pair)
            .await
            .unwrap();

        // Reduce the updated document.
        let input = TaskInput::DocumentId(document_id.clone());
        let _ = reduce_task(node.context.clone(), input).await.unwrap();
    }

    #[rstest]
    fn retrieves_and_sorts_entries(
        #[from(populate_store_config)]
        #[with(3, 1, generate_key_pairs(2))]
        config: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            // Create one node and materialize some documents on it.
            let mut node = manager.create().await;
            let documents = populate_and_materialize_unchecked(&mut node, &config).await;
            let schema = config.schema.clone();

            // Collect the values for the two authors and documents.
            let key_pair_a = config.authors.get(0).unwrap();
            let key_pair_b = config.authors.get(1).unwrap();

            let document_a = documents.get(0).unwrap().id();
            let document_b = documents.get(1).unwrap().id();

            // Compose the list of logs the a remote might need.
            let mut remote_needs_all = vec![
                (
                    key_pair_a.public_key(),
                    vec![(LogId::default(), SeqNum::default())],
                ),
                (
                    key_pair_b.public_key(),
                    vec![(LogId::default(), SeqNum::default())],
                ),
            ];

            // Compose expected returned entries for each document, identified by the document id
            // and sorted_index value. Entries should always be grouped into documents and then
            // ordered by their index number.
            let mut expected_entries = vec![
                (document_a.to_owned(), 0),
                (document_a.to_owned(), 1),
                (document_a.to_owned(), 2),
                (document_b.to_owned(), 0),
                (document_b.to_owned(), 1),
                (document_b.to_owned(), 2),
            ];

            // Sort to account for indeterminate document ids
            expected_entries.sort();

            // Retrieve entries and test against expected.
            retrieve_and_test_entries(&node, &remote_needs_all, &expected_entries).await;

            // Compose a different "needs" list now with values which should now only return a
            // section of the log.
            let remote_needs_some = [
                (
                    key_pair_a.public_key(),
                    vec![(LogId::default(), SeqNum::new(3).unwrap())],
                ),
                (
                    key_pair_b.public_key(),
                    vec![(LogId::default(), SeqNum::new(3).unwrap())],
                ),
            ];

            // Compose expected value and test.
            let mut expected_entries = vec![(document_a.to_owned(), 2), (document_b.to_owned(), 2)];
            // Sort to account for indeterminate document ids
            expected_entries.sort();

            retrieve_and_test_entries(&node, &remote_needs_some, &expected_entries).await;

            // We also want to make sure to with documents which contain concurrent updates. Here
            // we publish two operations into the documents past, causing branches to occur.

            // Create a new author.
            let key_pair = KeyPair::new();

            // Publish a concurrent update.
            update_and_materialize_document(
                &node,
                &key_pair,
                &schema,
                &document_a,
                vec![("username", "よつば")],
            )
            .await;

            // Publish another concurrent update.
            update_and_materialize_document(
                &node,
                &key_pair,
                &schema,
                &document_a,
                vec![("username", "ヂャンボ")],
            )
            .await;

            // Add the new author to the "needs" list
            remote_needs_all.push((
                key_pair.public_key(),
                vec![(LogId::default(), SeqNum::default())],
            ));

            // Now we expect 5 entries in document a still ordered by sorted index.
            let mut expected_entries = vec![
                (document_a.to_owned(), 0),
                (document_a.to_owned(), 1),
                (document_a.to_owned(), 2),
                (document_a.to_owned(), 3),
                (document_a.to_owned(), 4),
                (document_b.to_owned(), 0),
                (document_b.to_owned(), 1),
                (document_b.to_owned(), 2),
            ];

            // Sort to account for indeterminate document ids
            expected_entries.sort();

            // Retrieve entries and test against expected.
            retrieve_and_test_entries(&node, &remote_needs_all, &expected_entries).await;
        })
    }

    #[rstest]
    fn entry_responses_can_be_ingested(
        #[from(populate_store_config)]
        #[with(5, 2, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let schema = config.schema.clone();
            let target_set = SchemaIdSet::new(&vec![schema.id().to_owned()]);

            let mut node_a = manager.create().await;
            populate_and_materialize_unchecked(&mut node_a, &config).await;

            let node_b = manager.create().await;
            let schema_provider = node_b.context.schema_provider.clone();
            let _ = schema_provider.update(schema).await;
            let (tx, _) = broadcast::channel(50);
            let ingest = SyncIngest::new(schema_provider.clone(), tx);
            let strategy_a = LogHeightStrategy::new(&target_set, schema_provider.clone());

            let entry_responses: Vec<(EncodedEntry, Option<EncodedOperation>)> = strategy_a
                .entry_responses(&node_a.context.store, &[])
                .await
                .into_iter()
                .map(|message| match message {
                    Message::Entry(entry, operation) => (entry, operation),
                    _ => panic!(),
                })
                .collect();

            for (entry, operation) in &entry_responses {
                let result = ingest
                    .handle_entry(
                        &node_b.context.store,
                        entry,
                        operation
                            .as_ref()
                            .expect("All messages contain an operation"),
                    )
                    .await;

                assert!(result.is_ok());
            }
        });
    }

    #[rstest]
    fn calculates_log_heights(
        #[from(populate_store_config)]
        #[with(5, 2, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let target_set = SchemaIdSet::new(&vec![config.schema.id().to_owned()]);
            let mut node_a = manager.create().await;
            let documents = populate_and_materialize_unchecked(&mut node_a, &config).await;
            let document_ids: Vec<DocumentId> =
                documents.iter().map(AsDocument::id).cloned().collect();
            let strategy_a =
                LogHeightStrategy::new(&target_set, node_a.context.schema_provider.clone());

            let log_heights = strategy_a
                .local_log_heights(&node_a.context.store, &document_ids)
                .await;

            let expected_log_heights = config
                .authors
                .into_iter()
                .map(|key_pair| {
                    (
                        key_pair.public_key(),
                        vec![
                            (LogId::default(), SeqNum::new(5).unwrap()),
                            (LogId::new(1), SeqNum::new(5).unwrap()),
                        ],
                    )
                })
                .collect();

            assert_eq!(log_heights, expected_log_heights);
        });
    }

    #[rstest]
    // In the test we add the schema id of the `img` document to the target which is why this
    // seemingly empty target set returns log heights....
    #[case(vec![], vec![(LogId::new(5), SeqNum::new(1).unwrap())])] // LogId 5 is where the img document lives
    #[case(
        vec![SchemaId::Blob(1)],
        vec![
            (LogId::new(2), SeqNum::new(1).unwrap()), // LogId 2 is where the blob document lives
            (LogId::new(5), SeqNum::new(1).unwrap())
        ]
    )]
    #[case(
        vec![SchemaId::Blob(1), SchemaId::BlobPiece(1)],
        vec![
            (LogId::new(0), SeqNum::new(1).unwrap()), // LogId 0 is where a blob piece lives
            (LogId::new(1), SeqNum::new(1).unwrap()), // LogId 1 is where a blob piece lives
            (LogId::new(2), SeqNum::new(1).unwrap()),
            (LogId::new(5), SeqNum::new(1).unwrap())
        ]
    )]
    fn calculates_log_heights_based_on_dependencies(
        #[case] target_set_extension: Vec<SchemaId>,
        key_pair: KeyPair,
        #[case] expected_log_heights: Vec<(LogId, SeqNum)>,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let document_view_id = add_blob(
                &mut node_a,
                &generate_random_bytes(10),
                5,
                "text/plain".into(),
                &key_pair,
            )
            .await;

            let (schema, _) = add_schema_and_documents(
                &mut node_a,
                "img",
                vec![vec![(
                    "relation_to_blob",
                    document_view_id.into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            let mut target_set_schema = vec![schema.id().to_owned()];
            target_set_schema.extend(target_set_extension);
            let target_set = SchemaIdSet::new(&target_set_schema);
            let _ = node_a.context.schema_provider.update(schema).await;

            let strategy_a =
                LogHeightStrategy::new(&target_set, node_a.context.schema_provider.clone());

            let included_document_ids = strategy_a
                .included_document_ids(&node_a.context.store)
                .await;

            let log_heights = strategy_a
                .local_log_heights(&node_a.context.store, &included_document_ids)
                .await;

            let expected_log_heights =
                vec![(key_pair.public_key(), expected_log_heights.to_owned())]
                    .into_iter()
                    .collect();

            assert_eq!(log_heights, expected_log_heights);
        });
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![SchemaId::Blob(1)])]
    #[case(vec![SchemaId::Blob(1), SchemaId::BlobPiece(1)])]
    fn blobs_and_pieces_not_included_on_their_own(
        #[case] target_set_schema: Vec<SchemaId>,
        key_pair: KeyPair,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let document_view_id = add_blob(
                &mut node_a,
                &generate_random_bytes(10),
                2,
                "text/plain".into(),
                &key_pair,
            )
            .await;

            let (schema, _) = add_schema_and_documents(
                &mut node_a,
                "img",
                vec![vec![(
                    "relation_to_blob",
                    document_view_id.into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            let target_set = SchemaIdSet::new(&target_set_schema);
            let _ = node_a.context.schema_provider.update(schema).await;

            let strategy_a =
                LogHeightStrategy::new(&target_set, node_a.context.schema_provider.clone());

            let included_document_ids = strategy_a
                .included_document_ids(&node_a.context.store)
                .await;

            assert!(included_document_ids.is_empty());

            let log_heights = strategy_a
                .local_log_heights(&node_a.context.store, &included_document_ids)
                .await;

            assert!(log_heights.is_empty());
        });
    }
}
