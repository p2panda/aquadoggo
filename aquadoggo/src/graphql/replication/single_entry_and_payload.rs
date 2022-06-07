// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::*;

use crate::db::stores::StorageEntry;
use crate::graphql::Context as GraphQLContext;

use super::payload::Payload;
use super::Entry;

/// A p2panda entry with optional payload and the certificate pool required to verify it.
#[derive(Debug)]
pub struct SingleEntryAndPayload {
    pub entry: Entry,
    pub payload: Option<Payload>,
}

#[Object]
impl SingleEntryAndPayload {
    /// The entry
    async fn entry(&self) -> &Entry {
        &self.entry
    }

    /// The payload
    async fn payload(&self) -> Option<&Payload> {
        self.payload.as_ref()
    }

    /// Get the certificate pool for this entry that can be used to verify the entry is valid
    async fn certificate_pool<'a>(&self, ctx: &Context<'a>) -> Result<Vec<Entry>> {
        let ctx: &GraphQLContext = ctx.data()?;

        let result = ctx
            .replication_context
            .lock()
            .await
            .get_skiplinks(&self.entry)
            .await?;

        Ok(result)
    }
}

impl From<StorageEntry> for SingleEntryAndPayload {
    fn from(entry_and_payload: StorageEntry) -> Self {
        let entry = Entry(entry_and_payload.entry_signed().to_owned());
        let payload = entry_and_payload
            .operation_encoded()
            .map(|encoded| Payload(encoded.to_owned()));
        Self { entry, payload }
    }
}
