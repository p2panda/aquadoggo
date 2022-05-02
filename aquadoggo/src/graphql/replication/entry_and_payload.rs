use super::bamboo_entry::BambooEntry;
use super::payload::Payload;
use async_graphql::*;

#[derive(SimpleObject)]
pub struct EntryAndPayload {
    pub entry: BambooEntry,
    pub payload: Payload,
}
