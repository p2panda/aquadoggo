// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;
use std::marker::PhantomData;

use async_graphql::connection::{query, Connection, Edge, EmptyFields};
use async_graphql::Object;
use async_graphql::*;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod aliased_author;
pub mod author;
pub mod context;
pub mod entry;
pub mod entry_and_payload;
pub mod entry_hash;
pub mod log_id;
pub mod payload;
pub mod public_key;
pub mod sequence_number;
pub mod single_entry_and_payload;

use crate::db::stores::StorageEntry;
pub use aliased_author::AliasedAuthor;
pub use author::{Author, AuthorOrAlias};
pub use context::Context as ReplicationContext;
pub use entry::Entry;
pub use entry_and_payload::EntryAndPayload;
pub use entry_hash::EntryHash;
pub use log_id::LogId;
use p2panda_rs::storage_provider::traits::EntryStore;
pub use payload::Payload;
pub use public_key::PublicKey;
pub use sequence_number::SequenceNumber;
pub use single_entry_and_payload::SingleEntryAndPayload;

pub mod client;

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
    ) -> Result<Connection<usize, EntryAndPayload, EmptyFields, EmptyFields>> {
        let ctx: &Arc<Mutex<ReplicationContext<ES>>> = ctx.data()?;
        let author: AuthorOrAlias = author.try_into()?;
        query(after, None, first, None, |after, _, first, _| async move {
            let start =
                sequence_number.as_ref().as_u64() + after.map(|a| a as u64 + 1).unwrap_or(0);
            // TODO: clamp an upper limit here
            let first = first.unwrap_or(10);

            let edges = ctx
                .lock()
                .await
                .get_entries_newer_than_seq(log_id, author, sequence_number, first, start)
                .await?
                .into_iter()
                // FIXME: need to create a cursor from the entry here, not pass 0
                .map(|entry| Edge::new(0usize, entry.into()));

            let mut connection = Connection::new(false, start < first as u64);

            connection.append(edges);

            Result::<_, Error>::Ok(connection)
        })
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
