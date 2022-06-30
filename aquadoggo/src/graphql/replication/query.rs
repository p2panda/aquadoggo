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
        seq_num: scalars::SeqNum,
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
                let start: u64 = seq_num.as_u64() + after.map(|a| a.as_u64()).unwrap_or(0);
                let start_seq_num = SeqNum::new(start)?;

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
