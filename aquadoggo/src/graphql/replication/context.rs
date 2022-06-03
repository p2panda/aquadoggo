// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{anyhow, Result};
use async_graphql::ID;
use lru::LruCache;
use mockall::automock;
use p2panda_rs::entry::decode_entry;
use p2panda_rs::storage_provider::traits::EntryStore;

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

#[derive(Debug)]
pub struct Context<ES: 'static + EntryStore<StorageEntry>> {
    author_aliases: LruCache<ID, PublicKey>,
    next_alias: usize,
    entry_store: ES,
}

#[automock]
impl<ES: 'static + EntryStore<StorageEntry>> Context<ES> {
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
                let result = self.author_aliases.get(&id).cloned();
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
        let author = entry.as_ref().author();
        let entry = decode_entry(entry.as_ref(), None)?;
        let result = self
            .entry_store
            .get_certificate_pool(&author, entry.log_id(), entry.seq_num())
            .await?
            .into_iter()
            .map(|entry| entry.entry_signed().clone().into())
            .collect();

        Ok(result)
    }

    pub async fn get_entries_newer_than_seq(
        &mut self,
        log_id: LogId,
        author: AuthorOrAlias,
        sequence_number: SequenceNumber,
        max_number_of_entries: usize,
    ) -> Result<Vec<EntryAndPayload>> {
        let author = self.get_author(author)?;
        let result = self
            .entry_store
            .get_paginated_log_entries(
                &author.0,
                &log_id.0,
                sequence_number.as_ref(),
                max_number_of_entries,
            )
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
            AuthorOrAlias::PublicKey(public_key) => public_key,
        };
        Ok(author)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use p2panda_rs::identity::Author;

    use crate::graphql::replication::{
        Author as GraphQLAuthor, LogId as GraphQLLogId, PublicKey, SequenceNumber,
    };

    use super::super::testing::MockEntryStore;
    use super::Context;

    // TODO: test author aliases

    #[tokio::test]
    async fn entry_by_log_id_and_sequence() {
        let expected_log_id = 123;
        let expected_seq_num = 345u64;
        let expected_author_string =
            "7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982".to_string();

        let log_id: GraphQLLogId = expected_log_id.into();
        let sequence_number: SequenceNumber = expected_seq_num.try_into().unwrap();
        let author = Author::new(&expected_author_string).unwrap();
        let author_id = GraphQLAuthor {
            alias: None,
            public_key: Some(PublicKey(author)),
        };

        let mut mock_entry_store = MockEntryStore::new();
        mock_entry_store
            .expect_get_entry_at_seq_num()
            .withf(move |author, log_id, seq_num| {
                author.as_str() == expected_author_string
                    && log_id.as_u64() == expected_log_id
                    && seq_num.as_u64() == expected_seq_num
            })
            .times(1)
            .returning(|_, _, _| Ok(None));

        let mut context = Context::new(1, mock_entry_store);

        let result = context
            .entry_by_log_id_and_sequence(log_id, sequence_number, author_id.try_into().unwrap())
            .await;

        assert!(result.is_ok());
    }
}
