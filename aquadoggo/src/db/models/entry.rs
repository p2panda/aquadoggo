// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use p2panda_rs::entry::{EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

use serde::Serialize;
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

/// Struct representing the actual SQL row of `Entry`.
///
/// We store the u64 integer values of `log_id` and `seq_num` as strings since not all database
/// backend support large numbers.
#[derive(FromRow, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryRow {
    /// Public key of the author.
    pub author: String,

    /// Actual Bamboo entry data.
    pub entry_bytes: String,

    /// Hash of Bamboo entry data.
    pub entry_hash: String,

    /// Used log for this entry.
    pub log_id: String,

    /// Payload of entry, can be deleted.
    pub payload_bytes: Option<String>,

    /// Hash of payload data.
    pub payload_hash: String,

    /// Sequence number of this entry.
    pub seq_num: String,
}

impl AsRef<Self> for EntryRow {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Entry of an append-only log based on Bamboo specification. It describes the actual data in the
/// p2p network and is shared between nodes.
///
/// Bamboo entries are the main data type of p2panda. Entries are organized in a distributed,
/// single-writer append-only log structure, created and signed by holders of private keys and
/// stored inside the node database.
///
/// The actual entry data is kept in `entry_bytes` and separated from the `payload_bytes` as the
/// payload can be deleted without affecting the data structures integrity. All other fields like
/// `author`, `payload_hash` etc. can be retrieved from `entry_bytes` but are separately stored in
/// the database for faster querying.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    /// Public key of the author.
    pub author: Author,

    /// Actual Bamboo entry data.
    pub entry_bytes: String,

    /// Hash of Bamboo entry data.
    pub entry_hash: Hash,

    /// Used log for this entry.
    pub log_id: LogId,

    /// Payload of entry, can be deleted.
    pub payload_bytes: Option<String>,

    /// Hash of payload data.
    pub payload_hash: Hash,

    /// Sequence number of this entry.
    pub seq_num: SeqNum,
}

impl Entry {
    pub async fn insert(
        pool: &Pool,
        author: &Author,
        entry_bytes: &EntrySigned,
        entry_hash: &Hash,
        log_id: &LogId,
        payload_bytes: &OperationEncoded,
        payload_hash: &Hash,
        seq_num: &SeqNum,
    ) -> Result<bool> {
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
        .bind(author.as_str())
        .bind(entry_bytes.as_str())
        .bind(entry_hash.as_str())
        .bind(log_id.as_u64().to_string())
        .bind(payload_bytes.as_str())
        .bind(payload_hash.as_str())
        .bind(seq_num.as_u64().to_string())
        .execute(pool)
        .await?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Returns the latest Bamboo entry of an author's log.
    pub async fn latest(pool: &Pool, author: &Author, log_id: &LogId) -> Result<Option<Entry>> {
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
                cast(seq_num as unsigned) DESC
            LIMIT
                1
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .fetch_optional(pool)
        .await?;

        // Convert internal `EntryRow` to `Entry` with correct types
        let entry = row.map(|entry| Self::try_from(&entry).expect("Corrupt values found in entry"));

        Ok(entry)
    }

    pub async fn by_document(pool: &Pool, document: &Hash) -> Result<Vec<EntryRow>> {
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
                logs.document = $1
            ",
        )
        .bind(document.as_str())
        .fetch_all(pool)
        .await?;

        Ok(entries)
    }

    /// Return vector of all entries of a given schema
    // @TODO: This currently returns `EntryRow`, a better API would return `Entry` instead as it is
    // properly typed and `EntryRow` is only meant as an intermediate struct to deal with
    // databases. Here we still return `EntryRow` for the `queryEntries` RPC response (we want
    // `seq_num` and `log_id` to be strings). This should be changed as soon as we move over using
    // a GraphQL API.
    pub async fn by_schema(pool: &Pool, schema: &Hash) -> Result<Vec<EntryRow>> {
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
        .fetch_all(pool)
        .await?;

        Ok(entries)
    }

    /// Returns entry at sequence position within an author's log.
    pub async fn at_seq_num(
        pool: &Pool,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<Entry>> {
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
        .fetch_optional(pool)
        .await?;

        // Convert internal `EntryRow` to `Entry` with correct types
        let entry = row.map(|entry| Self::try_from(&entry).expect("Corrupt values found in entry"));

        Ok(entry)
    }
}

/// Convert SQL row representation `EntryRow` to typed `Entry` one.
impl TryFrom<&EntryRow> for Entry {
    type Error = crate::errors::Error;

    fn try_from(row: &EntryRow) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            author: Author::try_from(row.author.as_ref())?,
            entry_bytes: row.entry_bytes.clone(),
            entry_hash: row.entry_hash.parse()?,
            log_id: row.log_id.parse()?,
            payload_bytes: row.payload_bytes.clone(),
            payload_hash: row.payload_hash.parse()?,
            seq_num: row.seq_num.parse()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::LogId;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::Author;

    use super::Entry;

    use crate::test_helpers::initialize_db;

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[async_std::test]
    async fn latest_entry() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let log_id = LogId::new(1);

        let latest_entry = Entry::latest(&pool, &author, &log_id).await.unwrap();
        assert!(latest_entry.is_none());
    }

    #[async_std::test]
    async fn entries_by_schema() {
        let pool = initialize_db().await;

        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

        let entries = Entry::by_schema(&pool, &schema).await.unwrap();
        assert!(entries.is_empty());
    }

    #[async_std::test]
    async fn entries_by_document() {
        let pool = initialize_db().await;

        let document = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

        let entries = Entry::by_document(&pool, &document).await.unwrap();
        assert!(entries.is_empty());
    }
}
