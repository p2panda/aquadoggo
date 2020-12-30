use anyhow::Result;
use sqlx::{query, query_as, Done, FromRow};

use crate::db::Pool;

const FIRST_USER_LOG_ID: i64 = 1;

#[derive(FromRow, Debug)]
pub struct Log {
    author: String,
    log_id: i64,
    schema: String,
}

impl Log {
    /// Register a new log id for an author's schema.
    pub async fn register_log_id(
        pool: &Pool,
        author: &str,
        schema: &str,
        log_id: i64,
    ) -> Result<bool> {
        // Make sure its odd-numbered
        assert_eq!(log_id % 2, 1);

        let rows_affected = query(
            "
            INSERT INTO logs (author, log_id, schema)
            VALUES (?1, ?2, ?3)
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

    /// Determines the next free log id to register future user schemas of an author.
    ///
    /// User schema log ids are odd-numbered.
    pub async fn find_next_log_id(pool: &Pool, author: &str) -> Result<i64> {
        // @TODO: Do not determine the next one from the max log id as we might be missing free
        // ones before the largest (we can't assume that log ids from the wild are used
        // sequentially / without gaps).
        let last_log_id: (i64,) = query_as(
            "
            SELECT MAX(log_id)
            FROM logs
            WHERE author = ?1
            ",
        )
        .bind(author)
        .fetch_one(pool)
        .await?;

        let next_log_id = if last_log_id.0 == 0 {
            FIRST_USER_LOG_ID
        } else {
            last_log_id.0 + 2
        };

        Ok(next_log_id)
    }

    /// Returns the log_id of an author's schema. Finds the next one if not existing yet.
    ///
    /// Messages are separated in different logs per schema and author. This method checks if a log
    /// has already been registered for a certain schema and returns its id. If no log has been
    /// found it automatically returns the next possible free log id.
    pub async fn schema_log_id(pool: &Pool, author: &str, schema: &str) -> Result<i64> {
        let log = query_as::<_, Log>(
            "
            SELECT log_id
            FROM logs
            WHERE author = ?1 AND schema = ?2
            ",
        )
        .bind(author)
        .bind(schema)
        .fetch_optional(pool)
        .await?;

        let log_id = match log {
            Some(row) => row.log_id,
            None => Self::find_next_log_id(pool, author).await?,
        };

        Ok(log_id)
    }
}

#[cfg(test)]
mod tests {
    use super::Log;

    use crate::test_helpers::initialize_db;

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;
        let log_id = Log::schema_log_id(&pool, "", "").await.unwrap();
        assert_eq!(log_id, 1);
    }

    #[async_std::test]
    async fn user_log_ids() {
        let pool = initialize_db().await;
        let author = "baba";

        assert_eq!(Log::find_next_log_id(&pool, author).await.unwrap(), 1);
        Log::register_log_id(&pool, author, "bubu", 1)
            .await
            .unwrap();

        assert_eq!(Log::find_next_log_id(&pool, author).await.unwrap(), 3);
        Log::register_log_id(&pool, author, "bubu2", 3)
            .await
            .unwrap();

        assert_eq!(Log::find_next_log_id(&pool, author).await.unwrap(), 5);
    }
}
