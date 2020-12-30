use sqlx::{query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

#[derive(FromRow, Debug)]
pub struct Entry {
    pub author: String,
    pub entry_bytes: String,
    pub entry_hash: String,
    pub log_id: i64,
    pub payload_bytes: Option<String>,
    pub payload_hash: String,
    pub seqnum: i64,
}

impl Entry {
    /// Returns the latest bamboo entry of an author's log.
    pub async fn latest(pool: &Pool, author: &str, log_id: i64) -> Result<Option<Entry>> {
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
        author: &str,
        log_id: i64,
        seq_num: i64,
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

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;
        let latest_entry = Entry::latest(&pool, "", 1).await.unwrap();
        assert!(latest_entry.is_none());
    }
}
