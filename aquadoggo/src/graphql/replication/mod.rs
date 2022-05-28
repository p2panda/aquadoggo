// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_graphql::connection::{query, Connection, CursorType, Edge, EmptyFields};
use async_graphql::Object;
use async_graphql::*;
use mockall_double::double;
use p2panda_rs::entry::decode_entry;
use p2panda_rs::storage_provider::traits::EntryStore;
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

#[double]
pub use context::Context as ReplicationContext;
pub use entry::Entry;
pub use entry_and_payload::EntryAndPayload;
pub use entry_hash::EntryHash;
pub use log_id::LogId;
pub use payload::Payload;
pub use public_key::PublicKey;
pub use sequence_number::SequenceNumber;
pub use single_entry_and_payload::SingleEntryAndPayload;

#[derive(Debug)]
/// The root graphql object for replication
pub struct ReplicationRoot<ES> {
    entry_store: PhantomData<ES>,
}

impl<ES> ReplicationRoot<ES> {
    /// Create a new ReplicationRoot
    pub fn new() -> Self {
        Self {
            entry_store: PhantomData::default(),
        }
    }
}

#[Object]
impl<ES: 'static + EntryStore<StorageEntry> + Sync + Send> ReplicationRoot<ES> {
    /// Get an entry by its hash
    async fn entry_by_hash<'a>(
        &self,
        ctx: &Context<'a>,
        hash: EntryHash,
    ) -> Result<Option<SingleEntryAndPayload>> {
        let ctx: &Arc<Mutex<ReplicationContext<ES>>> = ctx.data()?;

        let result = ctx.lock().await.entry_by_hash(hash).await?;

        Ok(result)
    }

    /// Get any entries that are newer than the provided sequence_number for a given author and
    /// log_id
    async fn get_entries_newer_than_seq<'a>(
        &self,
        ctx: &Context<'a>,
        log_id: LogId,
        author: Author,
        sequence_number: SequenceNumber,
        first: Option<i32>,
        after: Option<String>,
    ) -> Result<Connection<SequenceNumber, EntryAndPayload, EmptyFields, EmptyFields>> {
        let ctx: &Arc<Mutex<ReplicationContext<ES>>> = ctx.data()?;
        let author: AuthorOrAlias = author.try_into()?;
        query(
            after,
            None,
            first,
            None,
            |after: Option<SequenceNumber>, _, first, _| async move {
                let start: u64 = sequence_number.as_u64() + after.map(|a| a.as_u64()).unwrap_or(0);

                // Limit the maximum number of entries to 10k, set a default value of 10
                let first = first.map(|n| n.clamp(0, 10000)).unwrap_or(10);

                let edges = ctx
                    .lock()
                    .await
                    .get_entries_newer_than_seq(log_id, author, sequence_number, first, start)
                    .await?
                    .into_iter()
                    .map(|entry| {
                        let decoded = decode_entry(entry.entry.as_ref(), None).unwrap();
                        let sequence_number = SequenceNumber(decoded.seq_num().clone());
                        Edge::new(sequence_number, entry.into())
                    });

                let mut connection = Connection::new(false, start < first as u64);

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
        let ctx: &Arc<Mutex<ReplicationContext<ES>>> = ctx.data()?;
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
        let ctx: &Arc<Mutex<ReplicationContext<ES>>> = ctx.data()?;
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

    use async_graphql::{
        connection::CursorType, EmptyMutation, EmptySubscription, Request, Schema, Value,
    };
    use tokio::sync::Mutex;

    use super::testing::MockEntryStore;
    use super::{AuthorOrAlias, ReplicationContext, ReplicationRoot, SequenceNumber};

    #[tokio::test]
    async fn get_entries_newer_than_seq_cursor_addition_is_ok() {
        // Main point of this test is make sure the cursor + sequence_number logic addition is
        // correct.
        let log_id = 3u64;
        let sequence_number = 123u64;
        let author_string =
            "7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982".to_string();
        let after = SequenceNumber::try_from(sequence_number).unwrap();
        let first = 5;
        let expected_start = sequence_number + after.as_u64();

        let gql_query = format!(
            "
        query{{
          getEntriesNewerThanSeq(logId: {}, author: {{publicKey: \"{}\" }}, sequenceNumber:{}, first: {}, after: \"{}\" ){{
            pageInfo {{
              hasNextPage
            }}
          }}
        }}",
            log_id, author_string, sequence_number, first, after.encode_cursor()
        );

        let mut replication_context: ReplicationContext<MockEntryStore> =
            ReplicationContext::default();

        // Prepare our main assertions.
        // - Checks that get_entries_newer_than_seq is called with the values we expect
        // - Checks that get_entries_newer_than_seq is called once
        // - Configures get_entries_newer_than_seq to return an empty Vec
        replication_context
            .expect_get_entries_newer_than_seq()
            .withf({
                let author_string = author_string.clone();

                move |log_id_, author_, sequence_number_, first_, start_| {
                    let author_matches = match author_ {
                        AuthorOrAlias::PublicKey(public_key) => {
                            public_key.0.as_str() == author_string
                        }
                        _ => false,
                    };
                    sequence_number_.as_u64() == sequence_number
                        && *start_ == expected_start
                        && log_id_.as_u64() == log_id
                        && author_matches
                        && *first_ == first
                }
            })
            .returning(|_, _, _, _, _| Ok(vec![]))
            .once();

        // Build up a schema with our mocks that can handle gql query strings
        let replication_root = ReplicationRoot::<MockEntryStore>::new();
        let schema = Schema::build(replication_root, EmptyMutation, EmptySubscription)
            .data(Arc::new(Mutex::new(replication_context)))
            .finish();

        // Act
        let result = schema.execute(Request::new(gql_query)).await;

        // Assert

        // Check that we get the Ok returned from get_entries_newer_than_seq
        assert!(result.is_ok());

        // The should not be a next page because we returned an empty vec from
        // get_entries_newer_than_seq
        let json_value = result.data.into_json().unwrap();
        let has_next_page = &json_value["getEntriesNewerThanSeq"]["pageInfo"]["hasNextPage"];
        assert!(!has_next_page.as_bool().unwrap());
    }
}
