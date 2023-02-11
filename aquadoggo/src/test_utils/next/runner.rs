// SPDX-License-Identifier: AGPL-3.0-or-later

use std::panic;
use std::sync::Arc;

use futures::Future;
use tokio::runtime::Builder;
use tokio::sync::Mutex;

use crate::db::Pool;
use crate::db::SqlStore;
use crate::test_utils::initialize_db_with_url;
use crate::test_utils::TestNode;

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
    async fn call(self, db: TestNode) {
        self(db).await
    }
}

#[async_trait::async_trait]
pub trait AsyncTestFnWithManager {
    async fn call(self, db: TestNodeManager);
}

#[async_trait::async_trait]
impl<FN, F> AsyncTestFnWithManager for FN
where
    FN: FnOnce(TestNodeManager) -> F + Sync + Send,
    F: Future<Output = ()> + Send,
{
    async fn call(self, db: TestNodeManager) {
        self(db).await
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

    pub async fn create(&self, url: &str) -> TestNode {
        let pool = initialize_db_with_url(url).await;

        // Initialise test store using pool.
        let store = SqlStore::new(pool.clone());

        let test_db = TestNode::new(store.clone());

        self.pools.lock().await.push(pool);
        test_db
    }
}

/// Method which provides a safe way to write tests with the ability to build many databases and
/// have their pool connections closed automatically when the test succeeds or fails.
///
/// Takes an (async) test function as an argument and passes over the `TestNodeManager`
/// instance which can be used to build databases from inside the tests.
pub fn with_db_manager_teardown<F: AsyncTestFnWithManager + Send + Sync + 'static>(test: F) {
    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("with_db_teardown")
        .build()
        .expect("Could not build tokio Runtime for test");

    // Instantiate the database manager
    let db_manager = TestNodeManager::new();

    // Get a handle onto it's collection of pools
    let pools = db_manager.pools.clone();

    runtime.block_on(async {
        // Spawn the test in a separate task to make sure we have control over the possible
        // panics which might happen inside of it
        let handle = tokio::task::spawn(async move {
            // Execute the actual test
            test.call(db_manager).await;
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
            Err(err) => panic::resume_unwind(err.into_panic()),
        };
    });
}
