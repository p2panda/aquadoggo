
use async_trait::async_trait;
use mockall::mock;
use mockall_double::double;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::EntryStorageError;
use p2panda_rs::storage_provider::traits::EntryStore;


use crate::db::stores::StorageEntry;

mock! {
    pub EntryStore {}
    #[async_trait]
    impl EntryStore<StorageEntry> for EntryStore {
        async fn insert_entry(&self, value: StorageEntry) -> Result<(), EntryStorageError>;

        async fn get_entry_at_seq_num(
            &self,
            author: &Author,
            log_id: &LogId,
            seq_num: &SeqNum,
            ) -> Result<Option<StorageEntry>, EntryStorageError>;

        async fn get_entry_by_hash(
            &self,
            hash: &Hash,
            ) -> Result<Option<StorageEntry>, EntryStorageError>;

        async fn get_latest_entry(
            &self,
            author: &Author,
            log_id: &LogId,
            ) -> Result<Option<StorageEntry>, EntryStorageError>;

        async fn get_entries_by_schema(
            &self,
            schema: &SchemaId,
            ) -> Result<Vec<StorageEntry>, EntryStorageError>;

        async fn get_paginated_log_entries(
            &self,
            author: &Author,
            log_id: &LogId,
            seq_num: &SeqNum,
            max_number_of_entries: usize,
            ) -> Result<Vec<StorageEntry>, EntryStorageError>;

        async fn get_certificate_pool(
            &self,
            author_id: &Author,
            log_id: &LogId,
            seq_num: &SeqNum,
            ) -> Result<Vec<StorageEntry>, EntryStorageError>;

    }
}
