// SPDX-License-Identifier: AGPL-3.0-or-later

use std::panic;

use futures::Future;
use tokio::runtime::Builder;

use crate::context::Context;
use crate::db::SqlStore;
use crate::schema::SchemaProvider;
use crate::test_utils::initialize_db;
use crate::test_utils::TestNode;
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
        let pool = initialize_db().await;
        let store = SqlStore::new(pool);

        // Construct the actual test node
        let node = TestNode {
            context: Context::new(
                store.clone(),
                Configuration::default(),
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
            Err(err) => panic::resume_unwind(err.into_panic()),
        };
    });
}
