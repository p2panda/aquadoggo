use crate::db::stores::StorageEntry;

use super::payload::Payload;
use super::Entry;
use crate::graphql::Context as GraphQLContext;
use async_graphql::*;

/// A p2panda entry with optional payload and the collection of skiplinks required to verify it.
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

    /// Get all the skiplinks for this entry that are required to verify the entry is valid
    async fn skiplinks<'a>(&self, ctx: &Context<'a>) -> Result<Vec<Entry>> {
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
    fn from(entry_row: StorageEntry) -> Self {
        let entry = Entry(entry_row.entry_signed().to_owned());
        let payload = entry_row
            .operation_encoded()
            .map(|encoded| Payload(encoded.to_owned()));
        Self { entry, payload }
    }
}
