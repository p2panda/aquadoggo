// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use futures::Future;
use p2panda_rs::identity::KeyPair;
use tokio::runtime::Builder;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::db::Pool;
use crate::db::SqlStore;
use crate::schema::SchemaProvider;
use crate::test_utils::TestNode;
use crate::test_utils::{initialize_db, initialize_sqlite_db};
use crate::Configuration;

#[async_trait::async_trait]
pub trait AsyncTestFn {
    async fn call(self, db: TestNode);
}

#[async_trait::async_trait]
impl<FN, F> AsyncTestFn for FN
where
    FN: FnOnce(TestNode) -> F + Sync + Send,
    F: Future<Output = ()> + Send,
{
    async fn call(self, node: TestNode) {
        self(node).await
    }
}

#[async_trait::async_trait]
pub trait AsyncTestFnWithManager {
    async fn call(self, manager: TestNodeManager);
}

#[async_trait::async_trait]
impl<FN, F> AsyncTestFnWithManager for FN
where
    FN: FnOnce(TestNodeManager) -> F + Sync + Send,
    F: Future<Output = ()> + Send,
{
    async fn call(self, manager: TestNodeManager) {
        self(manager).await
    }
}

/// A manager which can create many databases and retain a handle on their connection pools.
#[derive(Default)]
pub struct TestNodeManager {
    pools: Arc<Mutex<Vec<Pool>>>,
}

impl TestNodeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(&self) -> TestNode {
        self.create_with_config(Configuration::default()).await
    }

    pub async fn create_with_config(&self, config: Configuration) -> TestNode {
        let (_config, pool) = initialize_sqlite_db().await;

        // Initialise test store using pool.
        let store = SqlStore::new(pool.clone());

        let schema_provider = SchemaProvider::new(
            vec![], 
            config.allow_schema_ids.clone());

        // Construct the actual test node
        let test_node = TestNode {
            context: Context::new(
                store.clone(),
                KeyPair::new(),
                config,
                schema_provider,
            ),
        };

        self.pools.lock().await.push(pool);
        test_node
    }
}

/// Provides a safe way to write tests using a database which closes the pool connection
/// automatically when the test succeeds or fails.
///
/// Takes an (async) test function as an argument and passes over the `TestNode` instance
/// so it can be used inside of it.
pub fn test_runner<F: AsyncTestFn + Send + Sync + 'static>(test: F) {
    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("with_db_teardown")
        .build()
        .expect("Could not build tokio Runtime for test");

    runtime.block_on(async {
        // Initialise store
        let (_config, pool) = initialize_db().await;
        let store = SqlStore::new(pool);

        // Construct temporary blobs directory for the test runner
        let temp_dir = tempfile::TempDir::new()
            .expect("Could not create temporary test directory for blobs storage");

        // Construct node config supporting any schema
        let mut config = Configuration::default();
        config.blobs_base_path = temp_dir.path().to_path_buf();

        // Construct the actual test node
        let node = TestNode {
            context: Context::new(
                store.clone(),
                KeyPair::new(),
                config,
                SchemaProvider::default(),
            ),
        };

        // Get a handle of the underlying database connection pool
        let pool = node.context.store.pool.clone();

        // Spawn the test in a separate task to make sure we have control over the possible
        // panics which might happen inside of it
        let handle = tokio::task::spawn(async move {
            // Execute the actual test
            test.call(node).await;
        });

        // Get a handle of the task so we can use it later
        let result = handle.await;

        // Unwind the test by closing down the connection to the database pool. This will
        // be reached even when the test panicked
        pool.close().await;

        // Panic here when test failed. The test fails within its own async task and stays
        // there, we need to propagate it further to inform the test runtime about the result
        match result {
            Ok(_) => (),
            Err(err) => std::panic::resume_unwind(err.into_panic()),
        };
    });
}

/// Method which provides a safe way to write tests with the ability to build many databases and
/// have their pool connections closed automatically when the test succeeds or fails.
///
/// Takes an (async) test function as an argument and passes over the `TestNodeManager`
/// instance which can be used to build nodes with databases from inside the tests.
pub fn test_runner_with_manager<F: AsyncTestFnWithManager + Send + Sync + 'static>(test: F) {
    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("with_db_teardown")
        .build()
        .expect("Could not build tokio Runtime for test");

    // Instantiate the database manager
    let manager = TestNodeManager::new();

    // Get a handle onto its collection of pools
    let pools = manager.pools.clone();

    runtime.block_on(async {
        // Spawn the test in a separate task to make sure we have control over the possible
        // panics which might happen inside of it
        let handle = tokio::task::spawn(async move {
            // Execute the actual test
            test.call(manager).await;
        });

        // Get a handle of the task so we can use it later
        let result = handle.await;

        // Unwind the test by closing down the connections to all the database pools. This
        // will be reached even when the test panicked
        for pool in pools.lock().await.iter() {
            pool.close().await;
        }

        // Panic here when test failed. The test fails within its own async task and stays
        // there, we need to propagate it further to inform the test runtime about the result
        match result {
            Ok(_) => (),
            Err(err) => std::panic::resume_unwind(err.into_panic()),
        };
    });
}
