// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::migrate::MigrateDatabase;
use sqlx::Any;

use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::test_utils::TestConfiguration;

async fn db_from_config(config: TestConfiguration) -> (TestConfiguration, Pool) {
    drop_database(&config).await;
    create_database(&config.database_url).await.unwrap();

    let pool = connection_pool(&config.database_url, 1).await.unwrap();

    if run_pending_migrations(&pool).await.is_err() {
        pool.close().await;
        panic!("Database migration failed");
    }

    (config, pool)
}

/// Create test database.
pub async fn initialize_db() -> (TestConfiguration, Pool) {
    let config = TestConfiguration::new();
    db_from_config(config).await
}

pub async fn initialize_sqlite_db() -> (TestConfiguration, Pool) {
    let config = TestConfiguration::default();
    db_from_config(config).await
}

/// Delete test database.
pub async fn drop_database(config: &TestConfiguration) {
    if Any::database_exists(&config.database_url).await.unwrap() {
        Any::drop_database(&config.database_url).await.unwrap();
    }
}
