// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_graphql::connection::{query, Connection, CursorType, Edge, EmptyFields};
use async_graphql::Object;
use async_graphql::*;
use p2panda_rs::entry::decode_entry;
use p2panda_rs::storage_provider::traits::EntryStore as EntryStoreTrait;
use tokio::sync::Mutex;

use crate::db::stores::StorageEntry;

pub mod aliased_author;
pub mod author;
pub mod client;
pub mod context;
pub mod entry;
pub mod entry_and_payload;
pub mod entry_hash;
pub mod log_id;
pub mod payload;
pub mod public_key;
pub mod sequence_number;
pub mod single_entry_and_payload;

#[cfg(test)]
mod testing;

pub use aliased_author::AliasedAuthor;
pub use author::{Author, AuthorOrAlias};

#[cfg(test)]
pub use context::MockReplicationContext;
pub use context::ReplicationContext;
pub use entry::Entry;
pub use entry_and_payload::EntryAndPayload;
pub use entry_hash::EntryHash;
pub use log_id::LogId;
pub use payload::Payload;
pub use public_key::PublicKey;
pub use sequence_number::SequenceNumber;
pub use single_entry_and_payload::SingleEntryAndPayload;

#[derive(Debug, Default)]
/// The root graphql object for replication
pub struct ReplicationRoot<EntryStore> {
    entry_store: PhantomData<EntryStore>,
}

impl<EntryStore> ReplicationRoot<EntryStore> {
    /// Create a new ReplicationRoot
    pub fn new() -> Self {
        Self {
            entry_store: PhantomData::default(),
        }
    }
}

#[Object]
impl<EntryStore: 'static + EntryStoreTrait<StorageEntry> + Sync + Send>
    ReplicationRoot<EntryStore>
{
    /// Get an entry by its hash
    async fn entry_by_hash<'a>(
        &self,
        ctx: &Context<'a>,
        hash: EntryHash,
    ) -> Result<Option<SingleEntryAndPayload>> {
        let ctx: &Arc<Mutex<ReplicationContext<EntryStore>>> = ctx.data()?;
        let result = ctx.lock().await.entry_by_hash(hash).await?;
        Ok(result)
    }

    /// Get any entries that are newer than the provided sequence_number for a given author and
    /// log_id
    ///
    /// If you don't provide sequence_number then get all entries starting at the first
    async fn get_entries_newer_than_seq<'a>(
        &self,
        ctx: &Context<'a>,
        log_id: LogId,
        author: Author,
        sequence_number: Option<SequenceNumber>,
        first: Option<i32>,
        after: Option<String>,
    ) -> Result<Connection<SequenceNumber, EntryAndPayload, EmptyFields, EmptyFields>> {
        let ctx: &Arc<Mutex<ReplicationContext<EntryStore>>> = ctx.data()?;
        let author: AuthorOrAlias = author.try_into()?;
        query(
            after,
            None,
            first,
            None,
            |after: Option<SequenceNumber>, _, first, _| async move {
                let sequence_number = sequence_number.map(|seq| seq.as_u64()).unwrap_or_else(|| 0);

                // Add the sequence_number to the after cursor to get the starting sequence number.
                let start: u64 = sequence_number + after.map(|a| a.as_u64()).unwrap_or(0);

                // Limit the maximum number of entries to 10k, set a default value of 10
                let max_number_of_entries = first.map(|n| n.clamp(0, 10000)).unwrap_or(10);

                let edges = ctx
                    .lock()
                    .await
                    .get_entries_newer_than_seq(log_id, author, start, max_number_of_entries)
                    .await?
                    .into_iter()
                    .map(|entry| {
                        let decoded = decode_entry(entry.entry.as_ref(), None).unwrap();
                        let sequence_number = SequenceNumber(*decoded.seq_num());
                        Edge::new(sequence_number, entry)
                    })
                    .collect::<Vec<_>>();

                let has_next_page = edges.len() == max_number_of_entries;
                let mut connection = Connection::new(false, has_next_page);

                connection.append(edges);

                Result::<_, Error>::Ok(connection)
            },
        )
        .await
    }

    /// Get a single entry by its log_id, sequence_number and author
    async fn entry_by_log_id_and_sequence<'a>(
        &self,
        ctx: &Context<'a>,
        log_id: LogId,
        sequence_number: SequenceNumber,
        author: Author,
    ) -> Result<Option<SingleEntryAndPayload>> {
        let ctx: &Arc<Mutex<ReplicationContext<EntryStore>>> = ctx.data()?;
        let author: AuthorOrAlias = author.try_into()?;
        let result = ctx
            .lock()
            .await
            .entry_by_log_id_and_sequence(log_id, sequence_number, author)
            .await?;

        Ok(result)
    }

    /// Get aliases of the provided `public_keys` that you can use in future requests to save
    /// bandwidth.
    // Maybe this should be a mutation
    async fn author_aliases<'a>(
        &self,
        ctx: &Context<'a>,
        public_keys: Vec<PublicKey>,
    ) -> Result<Vec<AliasedAuthor>> {
        let ctx: &Arc<Mutex<ReplicationContext<EntryStore>>> = ctx.data()?;
        let result = ctx.lock().await.insert_author_aliases(public_keys);

        Ok(result)
    }
}

impl CursorType for SequenceNumber {
    type Error = AnyhowError;

    fn decode_cursor(s: &str) -> Result<Self, Self::Error> {
        let num: u64 = s.parse()?;
        let result = SequenceNumber::try_from(num)?;
        Ok(result)
    }

    fn encode_cursor(&self) -> String {
        self.as_u64().to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::sync::Arc;

    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
    use p2panda_rs::identity::Author;
    use rstest::rstest;
    use tokio::sync::Mutex;

    use crate::db::stores::test_utils::{
        populate_test_db, with_db_manager_teardown, PopulateDatabaseConfig, TestDatabaseManager,
    };
    use crate::SqlStorage;

    use super::{ReplicationContext, ReplicationRoot};

    #[rstest]
    #[case(100, None, None, None, true, 10)]
    #[case(10, Some(10), Some(5), None, false, 0)]
    #[case(14, Some(10), Some(5), None, false, 4)]
    #[case(15, Some(10), Some(5), None, true, 5)]
    #[case(16, Some(10), Some(5), None, true, 5)]
    fn get_entries_newer_than_seq_cursor(
        #[case] entries_in_log: usize,
        #[case] sequence_number: Option<u64>,
        #[case] first: Option<u64>,
        #[case] after: Option<u64>,
        #[case] expected_has_next_page: bool,
        #[case] expected_edges: usize,
    ) {
        with_db_manager_teardown(move |db_manager: TestDatabaseManager| async move {
            // Build and populate Billie's db
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

            // Construct the replication context, root and graphql schema.
            let replication_context: ReplicationContext<SqlStorage> =
                ReplicationContext::new(1, billie_db.store.clone());
            let replication_root = ReplicationRoot::<SqlStorage>::new();
            let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
                .data(Arc::new(Mutex::new(replication_context)))
                .finish();

            // Collect args needed for the query.
            let public_key = billie_db
                .test_data
                .key_pairs
                .first()
                .unwrap()
                .public_key()
                .to_owned();

            let author = Author::try_from(public_key).unwrap();
            let author_str: String = author.as_str().into();
            let log_id = 1u64;
            // Optional values are encpoded as `null`
            let sequence_number = sequence_number
                .map(|seq_num| seq_num.to_string())
                .unwrap_or_else(|| "null".to_string());
            let first = first
                .map(|seq_num| seq_num.to_string())
                .unwrap_or_else(|| "null".to_string());
            let after = after
                .map(|seq_num| format!("\"{}\"", seq_num))
                .unwrap_or_else(|| "null".to_string());

            // Construct the query.
            let gql_query = format!(
                "
                query{{
                    getEntriesNewerThanSeq(logId: {}, author: {{publicKey: \"{}\" }}, sequenceNumber: {}, first: {}, after: {} ){{
                        edges {{
                            cursor
                        }}
                        pageInfo {{
                            hasNextPage
                        }}
                    }}
                }}",
                    log_id, author_str, sequence_number, first, after
                );

            // Make the query.
            let result = schema.execute(Request::new(gql_query)).await;

            // Check that we get the Ok returned from get_entries_newer_than_seq
            assert!(result.is_ok());

            // Assert the returned hasNextPage and number of edges returned is what we expect.
            let json_value = result.data.into_json().unwrap();
            let edges = &json_value["getEntriesNewerThanSeq"]["edges"];
            assert_eq!(edges.as_array().unwrap().len(), expected_edges);
            let has_next_page = &json_value["getEntriesNewerThanSeq"]["pageInfo"]["hasNextPage"];
            assert_eq!(has_next_page.as_bool().unwrap(), expected_has_next_page);
        })
    }
}
