// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};
use p2panda_rs::storage_provider::traits::EntryStore;

use crate::db::provider::SqlStorage;
use crate::graphql::replication::EncodedEntryAndOperation;
use crate::graphql::scalars::EntryHash;

#[derive(Default, Debug, Copy, Clone)]
pub struct ReplicationRoot;

#[Object]
impl ReplicationRoot {
    /// Get an entry by its hash.
    async fn entry<'a>(
        &self,
        ctx: &Context<'a>,
        hash: EntryHash,
    ) -> Result<Option<EncodedEntryAndOperation>> {
        let store = ctx.data::<SqlStorage>()?;
        let result = store.get_entry_by_hash(&hash.into()).await?;
        Ok(result.map(EncodedEntryAndOperation::from))
    }
}
