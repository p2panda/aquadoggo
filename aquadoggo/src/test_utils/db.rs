// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::KeyPair;
use sqlx::migrate::MigrateDatabase;
use sqlx::Any;

use crate::config::Configuration;
use crate::context::Context;
use crate::db::SqlStore;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::schema::SchemaProvider;
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

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase {
    pub context: Context<SqlStore>,
    pub store: SqlStore,
    pub test_data: TestData,
}

impl TestDatabase {
    pub fn new(store: SqlStore) -> Self {
        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        TestDatabase {
            context,
            store,
            test_data: TestData::default(),
        }
    }
}

/// Data collected when populating a `TestDatabase` in order to easily check values which
/// would be otherwise hard or impossible to get through the store methods.
#[derive(Default)]
pub struct TestData {
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}
