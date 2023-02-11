// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::schema::FieldType;
use p2panda_rs::test_utils::memory_store::helpers::{populate_store, PopulateStoreConfig};
use sqlx::migrate::MigrateDatabase;
use sqlx::Any;

use crate::config::Configuration;
use crate::context::Context;
use crate::db::SqlStore;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::materializer::tasks::{dependency_task, reduce_task};
use crate::materializer::TaskInput;
use crate::schema::SchemaProvider;
use crate::test_utils::TEST_CONFIG;

use super::add_schema;

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
pub struct TestNode {
    pub context: Context<SqlStore>,
    pub store: SqlStore
}

impl TestNode {
    pub fn new(store: SqlStore) -> Self {
        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        TestNode {
            context,
            store
        }
    }
}

pub async fn populate_and_materialize(node: &mut TestNode, config: &PopulateStoreConfig) -> (Vec<KeyPair>, Vec<DocumentId>) {
    let (key_pairs, document_ids) = populate_store(&node.store, config).await;

    let schema_name = config.schema.name();
    let schema_fields: Vec<(&str, FieldType)> = config
        .schema
        .fields()
        .iter()
        .map(|(name, field)| (name.as_str(), field.clone()))
        .collect();

    add_schema(
        node,
        schema_name,
        schema_fields,
        key_pairs
            .get(0)
            .expect("There should be at least one key pair"),
    )
    .await;

    node.context
        .schema_provider
        .update(config.schema.clone())
        .await;

    for document_id in document_ids.clone() {
        let input = TaskInput::new(Some(document_id), None);
        let dependency_tasks = reduce_task(node.context.clone(), input.clone())
            .await
            .expect("Reduce document");

        // Run dependency tasks
        if let Some(tasks) = dependency_tasks {
            for task in tasks {
                dependency_task(node.context.clone(), task.input().to_owned())
                    .await
                    .expect("Run dependency task");
            }
        }
    }
    (key_pairs, document_ids)
}