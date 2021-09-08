// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;

use crate::config::Configuration;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::rpc::{build_rpc_api_service, start_rpc_server};
use crate::task::TaskManager;

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
pub struct Runtime {
    pool: Pool,
    task_manager: TaskManager,
}

impl Runtime {
    /// Start p2panda node with your configuration. This method can be used to run the node within
    /// other applications.
    pub async fn start(config: Configuration) -> Self {
        let mut task_manager = TaskManager::new();

        // Initialize database and get connection pool
        let pool = initialize_db(&config)
            .await
            .expect("Could not initialize database");

        // Create RPC API handler with shared database connection pool
        let rpc_api = build_rpc_api_service(pool.clone());

        // Start JSON RPC API server
        task_manager.spawn("JSON RPC Server", async move {
            start_rpc_server(&config, rpc_api).await?;
            Ok(())
        });

        Self { pool, task_manager }
    }

    /// Close all running concurrent tasks and wait until they are fully shut down.
    pub async fn shutdown(self) {
        // Close connection pool
        self.pool.close().await;

        // Wait until all tasks are shut down
        self.task_manager.shutdown().await;
    }
}
