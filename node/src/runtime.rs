use anyhow::Result;

use crate::config::Configuration;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::rpc::{ApiService, RpcServer};
use crate::task::TaskManager;

/// Makes sure database is created and migrated before returning connection pool.
async fn initialize_db(config: &Configuration) -> Result<Pool> {
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
    rpc_server: RpcServer,
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

        // Start JSON RPC API servers
        let io_handler = ApiService::io_handler(pool.clone());
        let rpc_server = RpcServer::start(&config, &mut task_manager, io_handler);

        Self {
            pool,
            task_manager,
            rpc_server,
        }
    }

    /// Close all running concurrent tasks and wait until they are fully shut down.
    pub async fn shutdown(self) {
        // Close connection pool
        self.pool.close().await;

        // Close RPC servers
        self.rpc_server.shutdown();

        // Wait until all tasks are shut down
        self.task_manager.shutdown().await;
    }
}
