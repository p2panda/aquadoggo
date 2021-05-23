use p2panda_rs::entry::{EntrySigned, LogId, SeqNum};
use p2panda_rs::message::MessageEncoded;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;

use serde::Serialize;
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

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
#[derive(FromRow, Debug, Serialize)]
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
        payload_bytes: &MessageEncoded,
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
        .bind(author)
        .bind(entry_bytes)
        .bind(entry_hash)
        .bind(log_id)
        .bind(payload_bytes)
        .bind(payload_hash)
        .bind(seq_num)
        .execute(pool)
        .await?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Returns the latest Bamboo entry of an author's log.
    pub async fn latest(pool: &Pool, author: &Author, log_id: &LogId) -> Result<Option<Entry>> {
        let latest_entry = query_as::<_, Entry>(
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
        .bind(author)
        .bind(log_id)
        .fetch_optional(pool)
        .await?;

        Ok(latest_entry)
    }

    /// Return vector of all entries of a given schema
    pub async fn by_schema(pool: &Pool, schema: &Hash) -> Result<Vec<Entry>> {
        let entries = query_as::<_, Entry>(
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
        .bind(schema)
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
        let entry = query_as::<_, Entry>(
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
        .bind(author)
        .bind(log_id)
        .bind(seq_num)
        .fetch_optional(pool)
        .await?;

        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::LogId;
    use p2panda_rs::identity::Author;
    use p2panda_rs::hash::Hash;

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
        assert!(entries.len() == 0);
    }
}
