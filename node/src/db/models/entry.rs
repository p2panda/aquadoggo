use sqlx::{query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;
use crate::types::{Author, EntryHash, LogId, SeqNum};

#[derive(FromRow, Debug)]
pub struct Entry {
    pub author: Author,
    pub entry_bytes: String,
    pub entry_hash: EntryHash,
    pub log_id: LogId,
    pub payload_bytes: Option<String>,
    pub payload_hash: EntryHash,
    pub seqnum: SeqNum,
}

impl Entry {
    /// Returns the latest bamboo entry of an author's log.
    pub async fn latest(pool: &Pool, author: &Author, log_id: &LogId) -> Result<Option<Entry>> {
        let latest_entry = query_as::<_, Entry>(
            "
            SELECT author, entry_bytes, entry_hash, log_id, payload_bytes, payload_hash, seqnum
            FROM entries
            WHERE author = ?1 AND log_id = ?2
            ORDER BY seqnum DESC
            LIMIT 1
            ",
        )
        .bind(author)
        .bind(log_id)
        .fetch_optional(pool)
        .await?;

        Ok(latest_entry)
    }

    /// Returns an entry at sequence number within an author's log.
    pub async fn at_seq_num(
        pool: &Pool,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<Entry>> {
        let entry = query_as::<_, Entry>(
            "
            SELECT author, entry_bytes, entry_hash, log_id, payload_bytes, payload_hash, seqnum
            FROM entries
            WHERE author = ?1 AND log_id = ?2 AND seqnum = ?3
            LIMIT 1
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
    use super::Entry;

    use crate::test_helpers::initialize_db;
    use crate::types::{Author, LogId};

    const TEST_AUTHOR: &str = "1a8a62c5f64eed987326513ea15a6ea2682c256ac57a418c1c92d96787c8b36e";

    #[async_std::test]
    async fn latest_entry() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let log_id = LogId::new(1).unwrap();

        let latest_entry = Entry::latest(&pool, &author, &log_id).await.unwrap();
        assert!(latest_entry.is_none());
    }
}
