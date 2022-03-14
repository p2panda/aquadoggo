// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;

use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::models::AsEntry;
use p2panda_rs::storage_provider::requests::AsEntryArgsRequest;
use p2panda_rs::storage_provider::responses::AsEntryArgsResponse;
use p2panda_rs::storage_provider::traits::{EntryStore, LogStore, StorageProvider};
use p2panda_rs::{entry::LogId, identity::Author};
use sqlx::{query, query_as, query_scalar};

use crate::db::models::Log;
use crate::db::Pool;
use crate::errors::Error;

use super::conversions::{EntryWithOperation, P2PandaLog};
use super::models::{Entry, EntryRow};

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

/// Trait which handles all storage actions relating to `Entries`.
#[async_trait]
impl EntryStore<EntryWithOperation> for Entry {
    /// Type representing an entry, must implement the `AsEntry` trait.
    type Entry = Entry;
    /// The error type
    type EntryError = Error;

    /// Insert an entry into storage.
    async fn insert(&self, entry: Self::Entry) -> Result<bool, Self::EntryError> {
        let db_entry: Self::Entry = entry.to_store_value().unwrap();
        let rows_affected = query(
            "
            INSERT INTO
                entries (
                    author,
                    entry_bytes,
                    entry_hash,
                    log_id,
                    payload_bytes,
                    payload_hash,
                    seq_num
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(db_entry.author.as_str())
        .bind(db_entry.entry_bytes.as_str())
        .bind(db_entry.entry_hash.as_str())
        .bind(db_entry.log_id.as_u64().to_string())
        .bind(db_entry.payload_bytes.unwrap().as_str())
        .bind(db_entry.payload_hash.as_str())
        .bind(db_entry.seq_num.as_u64().to_string())
        .execute(&self.pool)
        .await
        .map_err(|_| StorageProvider::Error)?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Returns entry at sequence position within an author's log.
    async fn entry_at_seq_num(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<Self::Entry>, Self::EntryError> {
        let row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
                AND seq_num = $3
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await?;

        // Convert internal `EntryRow` to `Entry` with correct types
        let entry =
            row.map(|entry| Self::Entry::try_from(entry).expect("Corrupt values found in entry"));

        Ok(entry)
    }

    /// Returns the latest Bamboo entry of an author's log.
    async fn latest_entry(
        &self,
        author: &Author,
        log_id: &LogId,
    ) -> Result<Option<Self::Entry>, Self::EntryError> {
        let row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
            ORDER BY
                seq_num DESC
            LIMIT
                1
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await?;

        // Convert internal `EntryRow` to `Entry` with correct types
        let entry = row.map(|entry: EntryRow| {
            Entry::try_from(entry)
                .map_err(|_| StorageProvider::Error)
                .unwrap()
        });

        Ok(entry)
    }

    /// Return vector of all entries of a given schema
    async fn by_schema(&self, schema: &Hash) -> Result<Vec<Self::Entry>, Self::EntryError> {
        let entries = query_as::<_, EntryRow>(
            "
            SELECT
                entries.author,
                entries.entry_bytes,
                entries.entry_hash,
                entries.log_id,
                entries.payload_bytes,
                entries.payload_hash,
                entries.seq_num
            FROM
                entries
            INNER JOIN logs
                ON (entries.log_id = logs.log_id
                    AND entries.author = logs.author)
            WHERE
                logs.schema = $1
            ",
        )
        .bind(schema.as_str())
        .fetch_all(&self.pool)
        .await?;

        Ok(entries)
    }
}

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
