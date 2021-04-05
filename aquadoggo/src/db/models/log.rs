use p2panda_rs::atomic::{Author, Hash, LogId};
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

/// Keeps track of which log_id has been used for which schema per author.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
#[derive(FromRow, Debug)]
pub struct Log {
    /// Public key of the author.
    author: Author,

    /// Log id used for the author's schema.
    log_id: LogId,

    /// Schema hash used by author.
    schema: Hash,
}

impl Log {
    /// Register any new log_id for an author's schema.
    ///
    /// The database will reject duplicate entries.
    #[allow(dead_code)]
    pub async fn insert(
        pool: &Pool,
        author: &Author,
        schema: &Hash,
        log_id: &LogId,
    ) -> Result<bool> {
        assert!(log_id.is_user_log());
        let rows_affected = query(
            "
            INSERT INTO
                logs (author, log_id, schema)
            VALUES
                (?1, ?2, ?3)
            ",
        )
        .bind(author)
        .bind(log_id)
        .bind(schema)
        .execute(pool)
        .await?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Determines the next unused user schema log_id of an author.
    pub async fn next_user_schema_log_id(pool: &Pool, author: &Author) -> Result<LogId> {
        // Get all log ids from this author
        let log_ids: Vec<LogId> = query_as::<_, LogId>(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = ?1
            ORDER BY
                log_id ASC
            ",
        )
        .bind(author)
        .fetch_all(pool)
        .await?;

        // Find next unused user schema log_id by comparing the sequence of known log ids with an
        // ideal sequence of subsequent log ids until we find a gap.
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Ignore system schema log ids
            if log_id.is_system_log() {
                continue;
            }

            // Success! Found unused log id
            if next_log_id != *log_id {
                break;
            }

            // Otherwise, try next possible log id
            next_log_id = next_log_id.next().unwrap();
        }

        Ok(next_log_id)
    }

    /// Returns the registered log_id of an author's schema.
    ///
    /// Messages are separated in different logs per schema and author. This method checks if a log
    /// has already been registered for a certain schema and returns its id.
    pub async fn get(pool: &Pool, author: &Author, schema: &Hash) -> Result<Option<LogId>> {
        // @TODO: Look up if system schema was used and return regarding log id
        let result = query_as::<_, LogId>(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = ?1
                AND schema = ?2
            ",
        )
        .bind(author)
        .bind(schema)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }

    /// Returns registered or possible log_id of an author's schema.
    ///
    /// If no log has been found for a USER schema it automatically returns the next unused log_id.
    /// SYSTEM schema log ids are pre-defined by the protocol specification.
    pub async fn find_schema_log_id(pool: &Pool, author: &Author, schema: &Hash) -> Result<LogId> {
        // Determine log_id for author's schema
        let schema_log_id = Log::get(pool, author, schema).await?;

        // Use result or find next possible log_id automatically
        let log_id = match schema_log_id {
            Some(value) => value,
            None => Log::next_user_schema_log_id(pool, author).await?,
        };

        Ok(log_id)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::atomic::{Author, Hash, LogId};

    use super::Log;

    use crate::test_helpers::{initialize_db, random_entry_hash};

    const TEST_AUTHOR: &str = "58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c";

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let schema = Hash::new(&random_entry_hash()).unwrap();

        let log_id = Log::find_schema_log_id(&pool, &author, &schema)
            .await
            .unwrap();

        assert_eq!(log_id, LogId::new(1));
    }

    #[async_std::test]
    async fn prevent_duplicate_log_ids() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let schema = Hash::new(&random_entry_hash()).unwrap();

        assert!(Log::insert(&pool, &author, &schema, &LogId::new(1))
            .await
            .is_ok());

        assert!(Log::insert(&pool, &author, &schema, &LogId::new(1))
            .await
            .is_err());
    }

    #[async_std::test]
    async fn user_log_ids() {
        let pool = initialize_db().await;

        // Mock author
        let author = Author::new(TEST_AUTHOR).unwrap();

        // Mock four different scheme hashes
        let schema_first = Hash::new(&random_entry_hash()).unwrap();
        let schema_second = Hash::new(&random_entry_hash()).unwrap();
        let schema_third = Hash::new(&random_entry_hash()).unwrap();
        let schema_system = Hash::new(&random_entry_hash()).unwrap();

        // Register two log ids at the beginning
        Log::insert(&pool, &author, &schema_system, &LogId::new(9))
            .await
            .unwrap();
        Log::insert(&pool, &author, &schema_first, &LogId::new(3))
            .await
            .unwrap();

        // Find next free user log id and register it
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(1));
        Log::insert(&pool, &author, &schema_second, &log_id)
            .await
            .unwrap();

        // Find next free user log id and register it
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(5));
        Log::insert(&pool, &author, &schema_third, &log_id)
            .await
            .unwrap();

        // Find next free user log id
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(7));
    }
}
