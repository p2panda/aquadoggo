use crate::db::stores::StorageEntry;

pub use super::aliased_author::AliasedAuthor;
pub use super::public_key::PublicKey;
use super::AuthorOrAlias;
use super::Entry;
use super::EntryAndPayload;
use super::EntryHash;
use super::LogId;
use super::SequenceNumber;
use super::SingleEntryAndPayload;
use anyhow::{anyhow, Result};
use async_graphql::ID;
use lru::LruCache;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::storage_provider::traits::EntryStore;

#[derive(Debug)]
pub struct Context<ES: EntryStore<StorageEntry>> {
    author_aliases: LruCache<ID, PublicKey>,
    next_alias: usize,
    entry_store: ES,
}

impl<ES: EntryStore<StorageEntry>> Context<ES> {
    pub fn new(author_aliases_cache_size: usize, entry_store: ES) -> Self {
        Self {
            author_aliases: LruCache::new(author_aliases_cache_size),
            next_alias: Default::default(),
            entry_store,
        }
    }

    pub fn insert_author_aliases(&mut self, public_keys: Vec<PublicKey>) -> Vec<AliasedAuthor> {
        public_keys
            .into_iter()
            .map(|public_key| {
                self.next_alias += 1;
                self.author_aliases
                    .put(ID(self.next_alias.to_string()), public_key.clone());
                AliasedAuthor {
                    public_key,
                    alias: self.next_alias.into(),
                }
            })
            .collect()
    }

    pub fn author_aliases_to_public_keys(
        &mut self,
        ids: Vec<ID>,
    ) -> Result<Vec<Option<PublicKey>>> {
        ids.into_iter()
            .map(|id| {
                let result = self.author_aliases.get(&id).map(|key| key.clone());
                Ok(result)
            })
            .collect()
    }

    pub async fn entry_by_log_id_and_sequence<'a>(
        &mut self,
        log_id: LogId,
        sequence_number: SequenceNumber,
        author_alias: AuthorOrAlias,
    ) -> Result<Option<SingleEntryAndPayload>> {
        let author = self.get_author(author_alias)?;

        let result = self
            .entry_store
            .get_entry_at_seq_num(&author.0, &log_id.0, &sequence_number.0)
            .await?
            .map(|entry| entry.into());

        Ok(result)
    }

    pub async fn entry_by_hash<'a>(
        &mut self,
        hash: EntryHash,
    ) -> Result<Option<SingleEntryAndPayload>> {
        let result = self
            .entry_store
            .get_entry_by_hash(&hash.into())
            .await?
            .map(|entry_row| entry_row.into());

        Ok(result)
    }

    pub async fn get_skiplinks<'a>(&mut self, entry: &Entry) -> Result<Vec<Entry>> {
        let entry = entry.as_ref();
        todo!();
        //let result = self.entry_store
        //    .get_all_lipmaa_entries_for_entry(&entry.author(), &entry.log_id() )
        //todo!()
    }

    pub async fn get_entries_newer_than_seq(
        &mut self,
        log_id: LogId,
        author: AuthorOrAlias,
        sequence_number: SequenceNumber,
        first: usize,
        after: u64,
    ) -> Result<Vec<EntryAndPayload>> {
        let author = self.get_author(author)?;
        let seq_num = SeqNum::new(sequence_number.as_ref().as_u64() + after)?;
        let result = self
            .entry_store
            .get_paginated_log_entries(&author.0, &log_id.0, &seq_num, first)
            .await?
            .into_iter()
            .map(|entry| entry.into())
            .collect();

        Ok(result)
    }

    fn get_author(&mut self, author_alias: AuthorOrAlias) -> Result<PublicKey, anyhow::Error> {
        let author = match author_alias {
            AuthorOrAlias::Alias(alias) => self
                .author_aliases
                .get(&alias)
                .ok_or(anyhow!(
                    "author alias did not exist, you may need to re-alias your authors"
                ))?
                .clone(),
            AuthorOrAlias::PublicKey(public_key) => public_key.clone(),
        };
        Ok(author)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::Context;
    use crate::db::models::EntryRow;
    use crate::db::stores::StorageEntry;
    use crate::graphql::replication::{
        Author as GraphQLAuthor, LogId as GraphQLLogId, PublicKey, SequenceNumber, ID,
    };
    use async_trait::async_trait;
    use mockall::mock;
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::Author;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::errors::EntryStorageError;
    use p2panda_rs::storage_provider::traits::EntryStore;
    use std::convert::TryInto;

//    #[tokio::test]
//    async fn entry_by_log_id_and_sequence() {
//        let expected_log_id = 123;
//        let expected_seq_num = 345u64;
//        let expected_author_id = 987u64;
//        let expected_author_string =
//            "7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982".to_string();
//
//        let log_id: GraphQLLogId = expected_log_id.into();
//        let sequence_number: SequenceNumber = expected_seq_num.try_into().unwrap();
//        let author = Author::new(&expected_author_string).unwrap();
//        let author_id = GraphQLAuthor {
//            alias: None,
//            public_key: Some(PublicKey(author)),
//        };
//
//        mock! {
//            pub MockEntryStore {}
//            #[async_trait]
//            impl EntryStore<StorageEntry> for MockEntryStore {
//                async fn insert_entry(&self, _value: StorageEntry) -> Result<bool, EntryStorageError>;
//
//                async fn get_entry_at_seq_num(
//                    &self,
//                    author: &Author,
//                    log_id: &LogId,
//                    seq_num: &SeqNum,
//                ) -> Result<Option<StorageEntry>, EntryStorageError>;
//
//                async fn latest_entry(
//                    &self,
//                    _author: &Author,
//                    _log_id: &LogId,
//                ) -> Result<Option<StorageEntry>, EntryStorageError>;
//
//            }
//        }
//
//        let mut mock_entry_store = MockMockEntryStore::new();
//        mock_entry_store
//            .expect_entry_at_seq_num()
//            .withf(move |author, log_id, seq_num| author.as_str() == expected_author_string)
//            .times(1)
//            .returning(|_, _, _| Ok(None));
//
//        let mut context = Context::new(1, mock_entry_store);
//
//        let result = context
//            .entry_by_log_id_and_sequence(log_id, sequence_number, author_id.try_into().unwrap())
//            .await;
//
//        println!("{:?}", result);
//        assert!(result.is_ok());
//    }
}
