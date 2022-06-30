// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Error;
use async_graphql::connection::{query, Connection, CursorType, Edge, EmptyFields};
use async_graphql::{Context, Object, Result};
use p2panda_rs::entry::{decode_entry, SeqNum};
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::db::provider::SqlStorage;
use crate::graphql::replication::response::EncodedEntryAndOperation;
use crate::graphql::scalars;

/// Maximum number of items per paginated query.
const MAX_PAGINATION_SIZE: usize = 10_000;

/// Default number of items per paginated query.
const DEFAULT_PAGINATION_SIZE: usize = 10;

/// Response type for paginated queries.
type ConnectionResult =
    Connection<scalars::SeqNum, EncodedEntryAndOperation, EmptyFields, EmptyFields>;

/// GraphQL queries for the Replication API.
#[derive(Default, Debug, Copy, Clone)]
pub struct ReplicationRoot;

#[Object]
impl ReplicationRoot {
    /// Get a single entry by its hash.
    async fn entry_by_hash<'a>(
        &self,
        ctx: &Context<'a>,
        hash: scalars::EntryHash,
    ) -> Result<Option<EncodedEntryAndOperation>> {
        let store = ctx.data::<SqlStorage>()?;
        let result = store.get_entry_by_hash(&hash.into()).await?;

        Ok(result.map(EncodedEntryAndOperation::from))
    }

    /// Get a single entry by its log id, sequence number and public key.
    async fn entry_by_log_id_and_seq_num<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(name = "logId", desc = "Log id of entry")] log_id: scalars::LogId,
        #[graphql(name = "seqNum", desc = "Sequence number of entry")] seq_num: scalars::SeqNum,
        #[graphql(name = "publicKey", desc = "Public key of the entry author")]
        public_key: scalars::PublicKey,
    ) -> Result<Option<EncodedEntryAndOperation>> {
        let store = ctx.data::<SqlStorage>()?;

        let result = store
            .get_entry_at_seq_num(&public_key.into(), &log_id.into(), &seq_num.into())
            .await?;

        Ok(result.map(EncodedEntryAndOperation::from))
    }

    /// Get any entries that are newer than the provided sequence number for a given author and log
    /// id.
    async fn get_entries_newer_than_seq_num<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(name = "logId", desc = "Log id of entries")] log_id: scalars::LogId,
        #[graphql(name = "publicKey", desc = "Public key of the author")]
        public_key: scalars::PublicKey,
        #[graphql(
            name = "seqNum",
            desc = "Query entries starting from this sequence number"
        )]
        seq_num: Option<scalars::SeqNum>,
        first: Option<i32>,
        after: Option<String>,
    ) -> Result<ConnectionResult> {
        let store = ctx.data::<SqlStorage>()?;

        query(
            after,
            None,
            first,
            None,
            |after: Option<scalars::SeqNum>, _, first, _| async move {
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

                let edges = store
                    .get_paginated_log_entries(
                        &public_key.into(),
                        &log_id.into(),
                        &start_seq_num.into(),
                        max_number_of_entries,
                    )
                    .await?
                    .into_iter()
                    .map(|entry_and_operation| {
                        // Unwrap as the entry was already validated before it entered the database
                        //
                        // @TODO: Is there a more efficient way than decoding the entry to access
                        // the sequence number?
                        let entry = decode_entry(entry_and_operation.entry_signed(), None).unwrap();

                        // Every pagination edge represents an entry and operation with the
                        // sequence number as the pagination cursor
                        Edge::new(
                            entry.seq_num().to_owned().into(),
                            entry_and_operation.into(),
                        )
                    })
                    .collect::<Vec<_>>();

                // @TODO: This returns true even when there is nothing on the next page, exactly
                // when the last page has the maximum number of elements
                let has_next_page = edges.len() == max_number_of_entries;
                let has_previous_page = start != 0;

                let mut connection = Connection::new(has_previous_page, has_next_page);
                connection.append(edges);

                Result::<_, Error>::Ok(connection)
            },
        )
        .await
    }
}

/// Use sequence numbers as cursor to paginate entry queries.
impl CursorType for scalars::SeqNum {
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
    use std::convert::TryFrom;

    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
    use p2panda_rs::identity::Author;
    use rstest::rstest;

    use crate::db::stores::test_utils::{
        populate_test_db, with_db_manager_teardown, PopulateDatabaseConfig, TestDatabaseManager,
    };

    use super::ReplicationRoot;

    #[rstest]
    #[case::default_params(20, None, None, None, true, 10)]
    #[case::no_edges_or_next_page(10, Some(10), Some(5), None, false, 0)]
    #[case::some_edges_no_next_page(14, Some(10), Some(5), None, false, 4)]
    #[case::edges_and_next_page(15, Some(10), Some(5), None, true, 5)]
    #[case::edges_and_next_page_again(16, Some(10), Some(5), None, true, 5)]
    fn get_entries_newer_than_seq_cursor(
        #[case] entries_in_log: usize,
        #[case] sequence_number: Option<u64>,
        #[case] first: Option<u64>,
        #[case] after: Option<u64>,
        #[case] expected_has_next_page: bool,
        #[case] expected_edges: usize,
    ) {
        with_db_manager_teardown(move |db_manager: TestDatabaseManager| async move {
            // Build and populate Billie's database
            let mut billie_db = db_manager.create("sqlite::memory:").await;

            populate_test_db(
                &mut billie_db,
                &PopulateDatabaseConfig {
                    no_of_entries: entries_in_log,
                    no_of_logs: 1,
                    no_of_authors: 1,
                    ..Default::default()
                },
            )
            .await;

            // Construct the replication context, root and graphql schema
            let replication_root = ReplicationRoot::default();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(billie_db.store)
                .finish();

            // Get public key from author of generated test data
            let public_key: String = {
                let key_from_db = billie_db
                    .test_data
                    .key_pairs
                    .first()
                    .unwrap()
                    .public_key()
                    .to_owned();

                let author = Author::try_from(key_from_db).unwrap();
                author.as_str().into()
            };

            // Test data has been written to first log
            let log_id = 1u64;

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
                r#"query {{
                    getEntriesNewerThanSeqNum(
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
                }}"#,
                log_id, public_key, seq_num, first, after
            );

            // Make the query
            let result = schema.execute(Request::new(gql_query.clone())).await;

            // Check that we get the Ok returned from get_entries_newer_than_seq
            assert!(
                result.is_ok(),
                "Query: {} \nResult: {:?}",
                gql_query,
                result
            );

            // Assert the returned hasNextPage and number of edges returned is what we expect
            let json_value = result.data.into_json().unwrap();
            let edges = &json_value["getEntriesNewerThanSeqNum"]["edges"];
            assert_eq!(edges.as_array().unwrap().len(), expected_edges);

            let has_next_page = &json_value["getEntriesNewerThanSeqNum"]["pageInfo"]["hasNextPage"];
            assert_eq!(has_next_page.as_bool().unwrap(), expected_has_next_page);
        })
    }
}
