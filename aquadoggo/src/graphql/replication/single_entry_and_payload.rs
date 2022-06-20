// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use async_graphql::*;
use tokio::sync::Mutex;

use crate::db::provider::SqlStorage;
use crate::db::stores::StorageEntry;
use crate::graphql::replication::context::ReplicationContext;

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

    /// Get the certificate pool for this entry that can be used to verify the entry is valid.
    async fn certificate_pool<'a>(&self, ctx: &Context<'a>) -> Result<Vec<Entry>> {
        let ctx: &Arc<Mutex<ReplicationContext<SqlStorage>>> = ctx.data()?;
        let result = ctx.lock().await.get_skiplinks(&self.entry).await?;
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
