// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::migrate::MigrateDatabase;
use sqlx::Any;

use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::test_utils::TEST_CONFIG;

/// Create test database.
pub async fn initialize_db() -> Pool {
    initialize_db_with_url(&TEST_CONFIG.database_url).await
}

/// Create test database.
pub async fn initialize_db_with_url(url: &str) -> Pool {
    // Reset database first
    drop_database().await;
    create_database(url).await.unwrap();

    // Create connection pool and run all migrations
    let pool = connection_pool(url, 25).await.unwrap();
    if run_pending_migrations(&pool).await.is_err() {
        pool.close().await;
    }

    pool
}

// Delete test database
pub async fn drop_database() {
    if Any::database_exists(&TEST_CONFIG.database_url)
        .await
        .unwrap()
    {
        Any::drop_database(&TEST_CONFIG.database_url).await.unwrap();
    }
}
