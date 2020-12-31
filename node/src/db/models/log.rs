use sqlx::{query, query_as, Done, FromRow};

use crate::db::Pool;
use crate::errors::Result;
use crate::types::{Author, LogId, Schema};

#[derive(FromRow, Debug)]
pub struct Log {
    author: Author,
    log_id: LogId,
    schema: Schema,
}

impl Log {
    /// Register a new log id for an author's schema.
    #[allow(dead_code)]
    pub async fn register_log_id(
        pool: &Pool,
        author: &Author,
        schema: &Schema,
        log_id: &LogId,
    ) -> Result<bool> {
        // Make sure its odd-numbered
        assert!(log_id.is_user_log());

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
    pub async fn find_next_log_id(pool: &Pool, author: &Author) -> Result<LogId> {
        // @TODO: Do not determine the next one from the max log id as we might be missing free
        // ones before the largest (we can't assume that log ids from the wild are used
        // sequentially / without gaps).
        let last_log_id: (LogId,) = query_as(
            "
            SELECT MAX(log_id)
            FROM logs
            WHERE author = ?1
            ",
        )
        .bind(author)
        .fetch_one(pool)
        .await?;

        // @TODO: Check for option not for zero value
        let next_log_id = if last_log_id.0.is_zero() {
            LogId::default()
        } else {
            last_log_id.0.next_user_log()?
        };

        Ok(next_log_id)
    }

    /// Returns the log_id of an author's schema. Finds the next one if not existing yet.
    ///
    /// Messages are separated in different logs per schema and author. This method checks if a log
    /// has already been registered for a certain schema and returns its id. If no log has been
    /// found it automatically returns the next possible free log id.
    pub async fn schema_log_id(pool: &Pool, author: &Author, schema: &Schema) -> Result<LogId> {
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
    use crate::types::{Author, Schema};

    #[async_std::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author =
            Author::new("58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c")
                .unwrap();
        let schema = Schema::new("lala").unwrap();

        let log_id = Log::schema_log_id(&pool, &author, &schema).await.unwrap();
        assert_eq!(log_id.0, 1);
    }

    #[async_std::test]
    async fn user_log_ids() {
        let pool = initialize_db().await;

        let author =
            Author::new("58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c")
                .unwrap();

        let schema_first = Schema::new("bubu").unwrap();
        let schema_second = Schema::new("baba").unwrap();

        let log_id = Log::find_next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id.0, 1);
        Log::register_log_id(&pool, &author, &schema_first, &log_id)
            .await
            .unwrap();

        let log_id = Log::find_next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id.0, 3);
        Log::register_log_id(&pool, &author, &schema_second, &log_id)
            .await
            .unwrap();

        let log_id = Log::find_next_log_id(&pool, &author).await.unwrap();
        assert_eq!(log_id.0, 5);
    }
}
