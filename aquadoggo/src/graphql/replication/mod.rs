use async_graphql::Object;
use async_graphql::*;

pub mod aliased_author;
pub mod bamboo_entry;
pub mod entry_and_payload;
pub mod entry_hash;
pub mod log_id;
pub mod payload;
pub mod public_key;
pub mod schema;
pub mod sequence_number;

pub use aliased_author::AliasedAuthor;
pub use bamboo_entry::BambooEntry;
pub use entry_and_payload::EntryAndPayload;
pub use entry_hash::EntryHash;
pub use log_id::LogId;
pub use payload::Payload;
pub use public_key::PublicKey;
pub use sequence_number::SequenceNumber;

pub mod client;

#[derive(Default)]
pub struct ReplicationRoot;

#[Object]
impl ReplicationRoot {
    async fn entry_by_hash<'a>(
        &self,
        _ctx: &Context<'a>,
        _hash: EntryHash,
    ) -> Result<Option<EntryAndPayload>> {
        Ok(None)
    }
    async fn entry_by_log_id_and_sequence<'a>(
        &self,
        _ctx: &Context<'a>,
        _log_id: LogId,
        _sequence_number: SequenceNumber,
        _author_id: ID,
    ) -> Result<Option<EntryAndPayload>> {
        todo!()
    }
    async fn author_aliases<'a>(
        &self,
        _ctx: &Context<'a>,
        _public_keys: Vec<PublicKey>,
    ) -> Result<Vec<AliasedAuthor>> {
        Ok(Vec::new())
    }
}
