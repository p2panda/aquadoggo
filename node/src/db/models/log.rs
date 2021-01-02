use sqlx::{query, query_as, Done, FromRow};

use crate::db::Pool;
use crate::errors::Result;
use crate::types::{Author, LogId, Schema};

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
    schema: Schema,
}

impl Log {
    /// Register any new log_id for an author's schema.
    ///
    /// The database will reject duplicate entries.
    #[allow(dead_code)]
    pub async fn register_log_id(
        pool: &Pool,
        author: &Author,
        schema: &Schema,
        log_id: &LogId,
    ) -> Result<bool> {
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

        Ok(rows_affected > 0)
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

        // Find next unused user schema log_id
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Ignore system schema log ids
            if log_id.is_system_log() {
                continue;
            }

            // This log_id is already used
            if next_log_id != *log_id {
                break;
            }

            // Iterate over the sequence of possible log_ids
            next_log_id = next_log_id.next().unwrap();
        }

        Ok(next_log_id)
    }

    /// Returns the registered log_id of an author's schema.
    ///
    /// Messages are separated in different logs per schema and author. This method checks if a log
    /// has already been registered for a certain schema and returns its id.
    ///
    /// If no log has been found for a USER schema it automatically returns the next unused log_id.
    /// SYSTEM schema log ids are constant and defined by the protocol specification.
    pub async fn schema_log_id(pool: &Pool, author: &Author, schema: &Schema) -> Result<LogId> {
        // @TODO: Check if system schema was used and return regarding log id
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

        // Return result or find next unused user schema log id
        let log_id = match result {
            Some(value) => value,
            None => Self::next_user_schema_log_id(pool, author).await?,
        };

        Ok(log_id)
    }
}

#[cfg(test)]
mod tests {
    use super::Log;

    use crate::test_helpers::{initialize_db, random_entry_hash};
    use crate::types::{Author, LogId, Schema};

    const TEST_AUTHOR: &str = "58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c";

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let schema = Schema::new(&random_entry_hash()).unwrap();

        let log_id = Log::schema_log_id(&pool, &author, &schema).await.unwrap();
        assert_eq!(log_id, LogId::new(1).unwrap());
    }

    #[async_std::test]
    async fn prevent_duplicate_log_ids() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();
        let schema = Schema::new(&random_entry_hash()).unwrap();

        assert!(
            Log::register_log_id(&pool, &author, &schema, &LogId::new(2).unwrap())
                .await
                .is_ok()
        );

        assert!(
            Log::register_log_id(&pool, &author, &schema, &LogId::new(2).unwrap())
                .await
                .is_err()
        );
    }

    #[async_std::test]
    async fn user_log_ids() {
        let pool = initialize_db().await;

        // Mock author
        let author = Author::new(TEST_AUTHOR).unwrap();

        // Mock four different scheme hashes
        let schema_first = Schema::new(&random_entry_hash()).unwrap();
        let schema_second = Schema::new(&random_entry_hash()).unwrap();
        let schema_third = Schema::new(&random_entry_hash()).unwrap();
        let schema_system = Schema::new(&random_entry_hash()).unwrap();

        // Register already two log ids at the beginning
        Log::register_log_id(&pool, &author, &schema_system, &LogId::new(2).unwrap())
            .await
            .unwrap();
        Log::register_log_id(&pool, &author, &schema_first, &LogId::new(3).unwrap())
            .await
            .unwrap();

        // Find next free user log id and register it
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(1).unwrap());
        Log::register_log_id(&pool, &author, &schema_second, &log_id)
            .await
            .unwrap();

        // Find next free user log id and register it
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(5).unwrap());
        Log::register_log_id(&pool, &author, &schema_third, &log_id)
            .await
            .unwrap();

        // Find next free user log id
        let log_id = Log::next_user_schema_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id, LogId::new(7).unwrap());
    }
}
