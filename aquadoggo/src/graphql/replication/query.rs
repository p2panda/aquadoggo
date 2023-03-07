// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Error;
use async_graphql::connection::{query, Connection, CursorType, Edge, EmptyFields};
use async_graphql::{Context, Object, Result};
use p2panda_rs::entry::traits::AsEntry;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::db::SqlStore;
use crate::graphql::replication::response::EncodedEntryAndOperation;
use crate::graphql::scalars;

/// Maximum number of items per paginated query.
const MAX_PAGINATION_SIZE: usize = 10_000;

/// Default number of items per paginated query.
const DEFAULT_PAGINATION_SIZE: usize = 10;

/// Response type for paginated queries.
type ConnectionResult =
    Connection<scalars::SeqNumScalar, EncodedEntryAndOperation, EmptyFields, EmptyFields>;

/// GraphQL queries for the Replication API.
#[derive(Default, Debug, Copy, Clone)]
pub struct ReplicationRoot;

#[Object]
impl ReplicationRoot {
    /// Get a single entry by its hash.
    async fn entry_by_hash<'a>(
        &self,
        ctx: &Context<'a>,
        hash: scalars::EntryHashScalar,
    ) -> Result<EncodedEntryAndOperation> {
        let store = ctx.data::<SqlStore>()?;
        let result = store.get_entry(&hash.clone().into()).await?;

        match result {
            Some(inner) => Ok(EncodedEntryAndOperation::from(inner)),
            None => Err(async_graphql::Error::new(format!(
                "Entry with hash {} could not be found",
                hash
            ))),
        }
    }

    /// Get a single entry by its log id, sequence number and public key.
    async fn entry_by_log_id_and_seq_num<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(name = "logId", desc = "Log id of entry")] log_id: scalars::LogIdScalar,
        #[graphql(name = "seqNum", desc = "Sequence number of entry")]
        seq_num: scalars::SeqNumScalar,
        #[graphql(name = "publicKey", desc = "Public key of the entry author")]
        public_key: scalars::PublicKeyScalar,
    ) -> Result<EncodedEntryAndOperation> {
        let store = ctx.data::<SqlStore>()?;

        let result = store
            .get_entry_at_seq_num(&public_key.into(), &log_id.into(), &seq_num.into())
            .await?;

        match result {
            Some(inner) => Ok(EncodedEntryAndOperation::from(inner)),
            None => Err(async_graphql::Error::new(format!(
                "Entry with log id {}, sequence number {} and public key {} could not be found",
                log_id, seq_num, public_key,
            ))),
        }
    }

    /// Get any entries that are newer than the provided sequence number for a given public key and
    /// log id.
    async fn entries_newer_than_seq_num<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(name = "logId", desc = "Log id of entries")] log_id: scalars::LogIdScalar,
        #[graphql(name = "publicKey", desc = "Public key of the author")]
        public_key: scalars::PublicKeyScalar,
        #[graphql(
            name = "seqNum",
            desc = "Query entries starting from this sequence number"
        )]
        seq_num: Option<scalars::SeqNumScalar>,
        first: Option<i32>,
        after: Option<String>,
    ) -> Result<ConnectionResult> {
        let store = ctx.data::<SqlStore>()?;

        query(
            after,
            None,
            first,
            None,
            |after: Option<scalars::SeqNumScalar>, _, first, _| async move {
                // Add `seq_num` to the `after` cursor to get starting sequence number
                let seq_num = seq_num.map(|seq| seq.as_u64()).unwrap_or_else(|| 0);
                let start: u64 = seq_num + after.map(|a| a.as_u64()).unwrap_or_else(|| 0);

                // `get_paginated_log_entries` is inclusive of seq_num. Whereas our seq_num should
                // not be included. So we add 1 to the the seq_num we were passed
                let start_seq_num = SeqNum::new(start + 1)?;

                // Limit the maximum number of entries and set a default
                let max_number_of_entries = first
                    .map(|n| n.clamp(0, MAX_PAGINATION_SIZE))
                    .unwrap_or(DEFAULT_PAGINATION_SIZE);

                let mut edges = store
                    .get_paginated_log_entries(
                        &public_key.into(),
                        &log_id.into(),
                        &start_seq_num,
                        max_number_of_entries,
                    )
                    .await?
                    .into_iter()
                    .map(|entry_and_operation| {
                        // Every pagination edge represents an entry and operation with the
                        // sequence number as the pagination cursor
                        Edge::new(
                            entry_and_operation.seq_num().to_owned().into(),
                            entry_and_operation.into(),
                        )
                    })
                    .collect::<Vec<_>>();

                // @TODO: This returns true even when there is nothing on the next page, exactly
                // when the last page has the maximum number of elements
                let has_next_page = edges.len() == max_number_of_entries;
                let has_previous_page = start > 0;

                let mut connection = Connection::new(has_previous_page, has_next_page);
                connection.edges.append(&mut edges);

                Result::<_, Error>::Ok(connection)
            },
        )
        .await
    }
}

/// Use sequence numbers as cursor to paginate entry queries.
impl CursorType for scalars::SeqNumScalar {
    type Error = Error;

    fn decode_cursor(str: &str) -> Result<Self, Self::Error> {
        Ok(str.parse()?)
    }

    fn encode_cursor(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::storage_provider::traits::EntryStore;
    use p2panda_rs::test_utils::fixtures::random_hash;
    use p2panda_rs::test_utils::memory_store::helpers::{populate_store, PopulateStoreConfig};
    use rstest::rstest;

    use crate::test_utils::{
        populate_store_config, test_runner, test_runner_with_manager, TestNode, TestNodeManager,
    };

    use super::ReplicationRoot;

    #[rstest]
    fn entry_by_hash(
        #[from(populate_store_config)]
        #[with(1, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations.
            let (key_pairs, _) = populate_store(&node.context.store, &config).await;
            let key_pair = key_pairs.get(0).expect("Should be one key pair");

            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(node.context.store.clone())
                .finish();

            // Retrieve an entry from the store.
            let entry = node
                .context
                .store
                .get_entry_at_seq_num(
                    &key_pair.public_key(),
                    &LogId::default(),
                    &SeqNum::default(),
                )
                .await
                .expect("The query to succeed")
                .expect("There to be a first entry");

            // Construct the query
            let gql_query = format!(
                r#"
                    query {{
                        entryByHash(hash: "{}") {{
                            entry
                            operation
                        }}
                    }}
                "#,
                entry.hash().as_str()
            );

            // Make the query
            let result = schema.execute(Request::new(gql_query.clone())).await;

            // Check that query was successful
            assert!(
                result.is_ok(),
                "Query: {} \nResult: {:?}",
                gql_query,
                result
            );
        });
    }

    #[rstest]
    fn entry_by_hash_not_found(#[from(random_hash)] random_hash: Hash) {
        test_runner(|node: TestNode| async move {
            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(node.context.store.clone())
                .finish();

            let gql_query = format!(
                r#"query {{ entryByHash(hash: "{}") {{ entry operation }} }}"#,
                random_hash.as_str()
            );

            // Make sure that query returns an error as entry was not found
            let result = schema.execute(Request::new(gql_query.clone())).await;
            assert!(result.is_err(), "{:?}", result);
        });
    }

    #[rstest]
    fn entry_by_log_id_and_seq_num(
        #[from(populate_store_config)]
        #[with(1, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations.
            let (key_pairs, _) = populate_store(&node.context.store, &config).await;
            // The key pair used when populating the store.
            let key_pair = key_pairs.get(0).expect("Should be one key pair");

            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(node.context.store.clone())
                .finish();

            // Construct the query
            let gql_query = format!(
                r#"
                    query {{
                        entryByLogIdAndSeqNum(logId: "{}", seqNum: "{}", publicKey: "{}") {{
                            entry
                            operation
                        }}
                    }}
                "#,
                0,
                1,
                key_pair.public_key().to_string()
            );

            // Make the query
            let result = schema.execute(Request::new(gql_query.clone())).await;

            // Check that query was successful
            assert!(
                result.is_ok(),
                "Query: {} \nResult: {:?}",
                gql_query,
                result
            );
        });
    }

    #[test]
    fn entry_by_log_id_and_seq_num_not_found() {
        test_runner(|node: TestNode| async move {
            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(node.context.store.clone())
                .finish();

            // Make a request with garbage data which will not exist in our test database
            let gql_query = format!(
                r#"query {{
                    entryByLogIdAndSeqNum(logId: "{}", seqNum: "{}", publicKey: "{}") {{
                        entry
                        operation
                    }}
                }}"#,
                2, 12, "64977654a6274f6157f2c3efe27ed89037c344f3af5c499e410946f50e25b6d7"
            );

            // Make sure that query returns an error as entry was not found
            let result = schema.execute(Request::new(gql_query.clone())).await;
            assert!(result.is_err(), "{:?}", result);
        });
    }

    #[rstest]
    #[case::default_params(20, None, None, None, true, 10)]
    #[case::no_edges_or_next_page(10, Some(10), Some(5), None, false, 0)]
    #[case::some_edges_no_next_page(14, Some(10), Some(5), None, false, 4)]
    #[case::edges_and_next_page(15, Some(10), Some(5), None, true, 5)]
    #[case::edges_and_next_page_again(16, Some(10), Some(5), None, true, 5)]
    fn entries_newer_than_seq_num_cursor(
        #[case] entries_in_log: usize,
        #[case] sequence_number: Option<u64>,
        #[case] first: Option<u64>,
        #[case] after: Option<u64>,
        #[case] expected_has_next_page: bool,
        #[case] expected_edges: usize,
    ) {
        test_runner_with_manager(move |manager: TestNodeManager| async move {
            // Construct Billie's node and populate the database.
            let billie = manager.create("sqlite::memory:").await;

            let (key_pairs, _) = populate_store(
                &billie.context.store,
                &PopulateStoreConfig {
                    no_of_entries: entries_in_log,
                    no_of_logs: 1,
                    no_of_public_keys: 1,
                    ..Default::default()
                },
            )
            .await;

            // Construct the replication context, root and graphql schema
            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(billie.context.store.clone())
                .finish();

            // Get public key from author of generated test data
            let public_key = key_pairs.first().unwrap().public_key();

            // Test data has been written to first log
            let log_id = 0u64;

            // Turn parameters into strings by wrapping them around quotation marks when existing,
            // otherwise give them "null" value
            let seq_num = sequence_number
                .map(|num| format!("\"{}\"", num))
                .unwrap_or_else(|| "null".to_string());
            let first = first
                .map(|num| num.to_string())
                .unwrap_or_else(|| "null".to_string());
            let after = after
                .map(|seq_num| format!("\"{}\"", seq_num))
                .unwrap_or_else(|| "null".to_string());

            // Construct the query
            let gql_query = format!(
                r#"
                    query {{
                        entriesNewerThanSeqNum(
                            logId: "{}",
                            publicKey: "{}",
                            seqNum: {},
                            first: {},
                            after: {}
                        ) {{
                            edges {{
                                cursor
                            }}
                            pageInfo {{
                                hasNextPage
                            }}
                        }}
                    }}
                "#,
                log_id, public_key, seq_num, first, after
            );

            // Make the query
            let result = schema.execute(Request::new(gql_query.clone())).await;

            // Check that we get the Ok returned from entries_newer_than_seq
            assert!(result.is_ok(), "{:?}", result);

            // Assert the returned hasNextPage and number of edges returned is what we expect
            let json_value = result.data.into_json().unwrap();
            let edges = &json_value["entriesNewerThanSeqNum"]["edges"];
            assert_eq!(edges.as_array().unwrap().len(), expected_edges);

            let has_next_page = &json_value["entriesNewerThanSeqNum"]["pageInfo"]["hasNextPage"];
            assert_eq!(has_next_page.as_bool().unwrap(), expected_has_next_page);
        })
    }
}
