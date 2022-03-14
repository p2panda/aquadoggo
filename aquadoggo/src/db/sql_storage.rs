// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;

use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::models::AsEntry;
use p2panda_rs::storage_provider::requests::AsEntryArgsRequest;
use p2panda_rs::storage_provider::responses::AsEntryArgsResponse;
use p2panda_rs::storage_provider::traits::{LogStore, StorageProvider};
use p2panda_rs::{entry::LogId, identity::Author};
use sqlx::{query, query_scalar};

use crate::db::models::Log;
use crate::db::Pool;
use crate::errors::Error;

use super::conversions::P2PandaLog;

pub struct SqlStorage {
    pool: Pool,
}

/// Trait which handles all storage actions relating to `Log`s.
#[async_trait]
impl LogStore<P2PandaLog> for SqlStorage {
    /// The error type
    type LogError = Error;
    /// The type representing a Log
    ///
    /// NB: Interestingly, there is no struct representing this in p2panda_rs,
    /// but that is all cool, thank you generics ;-p
    type Log = Log;

    /// Insert a log into storage.
    async fn insert(&self, value: Self::Log) -> Result<bool, Self::LogError> {
        todo!()
    }

    /// Get a log from storage
    async fn get(
        &self,
        author: &Author,
        document_id: &Hash,
    ) -> Result<Option<Self::Log>, Self::LogError> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = $1
                AND document = $2
            ",
        )
        .bind(author.as_str())
        .bind(document_id.as_str())
        .fetch_optional(&self.pool)
        .await?;

        // Wrap u64 inside of `P2PandaLog` instance
        let log_id: Option<Self::Log> =
            result.map(|str| str.parse().expect("Corrupt u64 integer found in database"));

        Ok(log_id)
    }

    /// Determines the next unused log_id of an author.
    async fn next_log_id(&self, author: &Author) -> Result<Self::Log, Self::LogError> {
        // Get all log ids from this author
        let mut result: Vec<String> = query_scalar(
            "
                    SELECT
                        log_id
                    FROM
                        logs
                    WHERE
                        author = $1
                    ",
        )
        .bind(author.as_str())
        .fetch_all(&self.pool)
        .await?;

        // Convert all strings representing u64 integers to `LogId` instances
        let mut log_ids: Vec<LogId> = result
            .iter_mut()
            .map(|str| str.parse().expect("Corrupt u64 integer found in database"))
            .collect();

        // The log id selection below expects log ids in sorted order. We can't easily use SQL
        // for this because log IDs are stored as `VARCHAR`, which doesn't sort numbers correctly.
        // A good solution would not require reading all existing log ids to find the next
        // available one. See this issue: https://github.com/p2panda/aquadoggo/issues/67
        log_ids.sort();

        // Find next unused document log by comparing the sequence of known log ids with an
        // sequence of subsequent log ids until we find a gap.
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Success! Found unused log id
            if next_log_id != *log_id {
                break;
            }

            // Otherwise, try next possible log id
            next_log_id = next_log_id.next().unwrap();
        }

        Ok(next_log_id)
    }
}

// /// Trait which handles all storage actions relating to `Entries`.
// #[async_trait]
// pub trait EntryStore<T> {
//     /// Type representing an entry, must implement the `AsEntry` trait.
//     type Entry: AsEntry<T>;
//     /// The error type
//     type EntryError: Debug;

//     /// Insert an entry into storage.
//     async fn insert(&self, value: Self::Entry) -> Result<bool, Self::EntryError>;

//     /// Returns entry at sequence position within an author's log.
//     async fn entry_at_seq_num(
//         &self,
//         author: &Author,
//         log_id: &LogId,
//         seq_num: &SeqNum,
//     ) -> Result<Option<Self::Entry>, Self::EntryError>;

//     /// Returns the latest Bamboo entry of an author's log.
//     async fn latest_entry(
//         &self,
//         author: &Author,
//         log_id: &LogId,
//     ) -> Result<Option<Self::Entry>, Self::EntryError>;

//     /// Return vector of all entries of a given schema
//     async fn by_schema(&self, schema: &Hash) -> Result<Vec<Self::Entry>, Self::EntryError>;

//     /// Determine skiplink entry hash ("lipmaa"-link) for entry in this log, return `None` when no
//     /// skiplink is required for the next entry.
//     async fn determine_skiplink(
//         &self,
//         entry: &Self::Entry,
//     ) -> Result<Option<Hash>, Self::EntryError>;
// }

// /// All other methods needed to be implemented by a p2panda `StorageProvider`
// #[async_trait]
// pub trait StorageProvider<T>: LogStore + EntryStore<T> {
//     /// The error type
//     type Error: Debug + Send + Sync;

//     /// Returns the related document for any entry.
//     ///
//     /// Every entry is part of a document and, through that, associated with a specific log id used
//     /// by this document and author. This method returns that document id by looking up the log
//     /// that the entry was stored in.
//     async fn get_document_by_entry(&self, entry_hash: &Hash) -> Result<Option<Hash>, Self::Error>;
// }
