// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::bus::ServiceMessage;
use crate::config::Configuration;
use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::db::traits::SchemaStore;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::http::http_service;
use crate::manager::ServiceManager;
use crate::materializer::materializer_service;
use crate::replication::replication_service;
use crate::schema::SchemaProvider;

/// Makes sure database is created and migrated before returning connection pool.
async fn initialize_db(config: &Configuration) -> Result<Pool> {
    // Find SSL certificate locations on the system for OpenSSL for TLS
    openssl_probe::init_ssl_cert_env_vars();

    // Create database when not existing
    create_database(&config.database_url.clone().unwrap()).await?;

    // Create connection pool
    let pool = connection_pool(
        &config.database_url.clone().unwrap(),
        config.database_max_connections,
    )
    .await?;

    // Run pending migrations
    run_pending_migrations(&pool).await?;

    Ok(pool)
}

/// Main runtime managing the p2panda node process.
#[allow(missing_debug_implementations)]
pub struct Node {
    pool: Pool,
    manager: ServiceManager<Context, ServiceMessage>,
}

impl Node {
    /// Start p2panda node with your configuration. This method can be used to run the node within
    /// other applications.
    pub async fn start(config: Configuration) -> Self {
        // Initialize database and get connection pool.
        let pool = initialize_db(&config)
            .await
            .expect("Could not initialize database");

        // Prepare storage and schema providers using connection pool.
        let store = SqlStorage::new(pool.clone());
        let schemas = SchemaProvider::new(store.get_all_schema().await.unwrap());

        // Create service manager with shared data between services.
        let context = Context::new(store, config, schemas);
        let mut manager = ServiceManager::<Context, ServiceMessage>::new(1024, context);

        // Start materializer service.
        if manager.add("materializer", materializer_service).await.is_err() {
            panic!("Failed starting materialiser service");
        }
        // Start replication service
        if manager.add("replication", replication_service).await.is_err() {
            panic!("Failed starting replication service");
        }

        // Start HTTP server with GraphQL API
        if manager.add("http", http_service).await.is_err() {
            panic!("Failed starting HTTP service");
        }

        Self { pool, manager }
    }

    /// This future resolves when at least one system service stopped.
    ///
    /// It can be used to exit the application as a stopped service usually means that something
    /// went wrong.
    pub async fn on_exit(&self) {
        self.manager.on_exit().await;
    }

    /// Close all running concurrent tasks and wait until they are fully shut down.
    pub async fn shutdown(self) {
        // Wait until all tasks are shut down
        self.manager.shutdown().await;

        // Close connection pool
        self.pool.close().await;
    }
}
