// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::identity::KeyPair;
use tokio::sync::mpsc::Receiver;

use crate::api::{NodeEvent, NodeInterface};
use crate::bus::ServiceMessage;
use crate::config::Configuration;
use crate::context::Context;
use crate::db::SqlStore;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::http::http_service;
use crate::manager::ServiceManager;
use crate::materializer::materializer_service;
use crate::network::network_service;
use crate::replication::replication_service;
use crate::schema::SchemaProvider;
use crate::LockFile;

/// Capacity of the internal broadcast channel used to communicate between services.
const SERVICE_BUS_CAPACITY: usize = 512_000;

/// Makes sure database is created and migrated before returning connection pool.
async fn initialize_db(config: &Configuration) -> Result<Pool> {
    // Find SSL certificate locations on the system for OpenSSL for TLS
    openssl_probe::init_ssl_cert_env_vars();

    // Create database when not existing
    create_database(&config.database_url).await?;

    // Create connection pool
    let pool = connection_pool(&config.database_url, config.database_max_connections).await?;

    // Run pending migrations
    run_pending_migrations(&pool).await?;

    Ok(pool)
}

/// Main runtime managing the p2panda node process.
#[allow(missing_debug_implementations)]
pub struct Node {
    pool: Pool,
    manager: ServiceManager<Context, ServiceMessage>,
    api: NodeInterface,
}

impl Node {
    /// Start p2panda node with your configuration. This method can be used to run the node within
    /// other applications.
    pub async fn start(key_pair: KeyPair, config: Configuration) -> Self {
        // Initialize database and get connection pool
        let pool = initialize_db(&config)
            .await
            .expect("Could not initialize database");

        // Prepare storage and schema providers using connection pool
        let store = SqlStore::new(pool.clone());

        // Initiate the SchemaProvider with all currently known schema from the store.
        //
        // If a list of allowed schema ids is provided then only schema identified in this list
        // will be added to the provider and supported by the node.
        let application_schema = store.get_all_schema().await.unwrap();
        let schema_provider =
            SchemaProvider::new(application_schema, config.allow_schema_ids.clone());

        // Create service manager with shared data between services
        let context = Context::new(store, key_pair, config, schema_provider);
        let mut manager =
            ServiceManager::<Context, ServiceMessage>::new(SERVICE_BUS_CAPACITY, context.clone());

        // Start materializer service
        if manager
            .add("materializer", materializer_service)
            .await
            .is_err()
        {
            panic!("Failed starting materialiser service");
        }

        // Start HTTP server with GraphQL API
        if manager.add("http", http_service).await.is_err() {
            panic!("Failed starting HTTP service");
        }

        // Start network service
        if manager.add("network", network_service).await.is_err() {
            panic!("Failed starting network service");
        }

        // Start replication service syncing data with other nodes
        if manager
            .add("replication", replication_service)
            .await
            .is_err()
        {
            panic!("Failed starting replication service");
        }

        // Create a low-level interface which can be exposed so developers can interact with the
        // internal store and service bus
        let api = NodeInterface::new(context, manager.get_sender());

        Self { pool, manager, api }
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

    /// Utility method to publish multiple operations and entries in the node database.
    ///
    /// This method is especially useful for migrating schemas or seeding initial data. Lock files
    /// with schema data can be created with the p2panda command line tool `fishy`.
    ///
    /// Returns `true` if migration took place or `false` if no migration was required.
    pub async fn migrate(&self, lock_file: LockFile) -> Result<bool> {
        self.api.migrate(lock_file).await
    }

    /// Subscribe to channel reporting on significant node events which can be interesting for
    /// clients, for example when peers connect or disconnect.
    pub async fn subscribe(&self) -> Receiver<NodeEvent> {
        self.api.subscribe().await
    }
}
