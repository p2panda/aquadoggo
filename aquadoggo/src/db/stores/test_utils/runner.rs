// SPDX-License-Identifier: AGPL-3.0-or-later

use std::panic;
use std::sync::Arc;

use futures::Future;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::test_utils::constants::{test_fields, SCHEMA_ID};
use rstest::fixture;
use tokio::runtime::Builder;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::db::stores::test_utils::{
    populate_test_db, PopulateDatabaseConfig, TestData, TestDatabase,
};
use crate::db::Pool;
use crate::test_helpers::{initialize_db, initialize_db_with_url};
use crate::{Configuration, SchemaProvider};

#[async_trait::async_trait]
pub trait AsyncTestFn {
    async fn call(self, db: TestDatabase);
}

#[async_trait::async_trait]
impl<FN, F> AsyncTestFn for FN
where
    FN: FnOnce(TestDatabase) -> F + Sync + Send,
    F: Future<Output = ()> + Send,
{
    async fn call(self, db: TestDatabase) {
        self(db).await
    }
}

#[async_trait::async_trait]
pub trait AsyncTestFnWithManager {
    async fn call(self, db: TestDatabaseManager);
}

#[async_trait::async_trait]
impl<FN, F> AsyncTestFnWithManager for FN
where
    FN: FnOnce(TestDatabaseManager) -> F + Sync + Send,
    F: Future<Output = ()> + Send,
{
    async fn call(self, db: TestDatabaseManager) {
        self(db).await
    }
}

// @TODO: I'm keeping this here for now as otherwise we would need to refactor _all_ the tests using it.
//
// We may still want to keep this "single database" runner injected through `rstest` but in any case
// probably best to consider that in a different PR.
pub struct TestDatabaseRunner {
    config: PopulateDatabaseConfig,
}

impl TestDatabaseRunner {
    /// Provides a safe way to write tests using a database which closes the pool connection
    /// automatically when the test succeeds or fails.
    ///
    /// Takes an (async) test function as an argument and passes over the `TestDatabase` instance
    /// so it can be used inside of it.
    pub fn with_db_teardown<F: AsyncTestFn + Send + Sync + 'static>(&self, test: F) {
        let runtime = Builder::new_current_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("with_db_teardown")
            .build()
            .expect("Could not build tokio Runtime for test");

        runtime.block_on(async {
            // Initialise test database
            let pool = initialize_db().await;
            let store = SqlStorage::new(pool);
            let context = Context::new(
                store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            let mut db = TestDatabase {
                context,
                store,
                test_data: TestData::default(),
            };

            // Populate the test db
            populate_test_db(&mut db, &self.config).await;

            // Get a handle of the underlying database connection pool
            let pool = db.store.pool.clone();

            // Spawn the test in a separate task to make sure we have control over the possible
            // panics which might happen inside of it
            let handle = tokio::task::spawn(async move {
                // Execute the actual test
                test.call(db).await;
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
}

/// A manager which can create many databases and retain a handle on their connection pools.
#[derive(Default)]
pub struct TestDatabaseManager {
    pools: Arc<Mutex<Vec<Pool>>>,
}

impl TestDatabaseManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(&self, url: &str) -> TestDatabase {
        let pool = initialize_db_with_url(url).await;

        // Initialise test store using pool.
        let store = SqlStorage::new(pool.clone());

        let test_db = TestDatabase::new(store.clone());

        self.pools.lock().await.push(pool);
        test_db
    }
}

/// Fixture for constructing a storage provider instance backed by a pre-populated database.
///
/// Returns a `TestDatabaseRunner` that bootstraps a safe async test environment connecting to a
/// database. It makes sure the runner disconnects properly from the connection pool after the test
/// succeeded or even failed.
///
/// Passed parameters define what the database should contain. The first entry in each log contains
/// a valid CREATE operation following entries contain duplicate UPDATE operations. If the
/// with_delete flag is set to true the last entry in all logs contain be a DELETE operation.
#[fixture]
pub fn test_db(
    // Number of entries per log/document
    #[default(0)] no_of_entries: usize,
    // Number of logs for each author
    #[default(0)] no_of_logs: usize,
    // Number of authors, each with logs populated as defined above
    #[default(0)] no_of_authors: usize,
    // A boolean flag for wether all logs should contain a delete operation
    #[default(false)] with_delete: bool,
    // The schema used for all operations in the db
    #[default(SCHEMA_ID.parse().unwrap())] schema: SchemaId,
    // The fields used for every CREATE operation
    #[default(test_fields())] create_operation_fields: Vec<(&'static str, OperationValue)>,
    // The fields used for every UPDATE operation
    #[default(test_fields())] update_operation_fields: Vec<(&'static str, OperationValue)>,
) -> TestDatabaseRunner {
    let config = PopulateDatabaseConfig {
        no_of_entries,
        no_of_logs,
        no_of_authors,
        with_delete,
        schema,
        create_operation_fields,
        update_operation_fields,
    };

    TestDatabaseRunner { config }
}

/// Fixture for passing in `PopulateDatabaseConfig` into tests.
#[fixture]
pub fn test_db_config(
    // Number of entries per log/document
    #[default(0)] no_of_entries: usize,
    // Number of logs for each author
    #[default(0)] no_of_logs: usize,
    // Number of authors, each with logs populated as defined above
    #[default(0)] no_of_authors: usize,
    // A boolean flag for wether all logs should contain a delete operation
    #[default(false)] with_delete: bool,
    // The schema used for all operations in the db
    #[default(SCHEMA_ID.parse().unwrap())] schema: SchemaId,
    // The fields used for every CREATE operation
    #[default(test_fields())] create_operation_fields: Vec<(&'static str, OperationValue)>,
    // The fields used for every UPDATE operation
    #[default(test_fields())] update_operation_fields: Vec<(&'static str, OperationValue)>,
) -> PopulateDatabaseConfig {
    PopulateDatabaseConfig {
        no_of_entries,
        no_of_logs,
        no_of_authors,
        with_delete,
        schema,
        create_operation_fields,
        update_operation_fields,
    }
}

/// Method which provides a safe way to write tests with the ability to build many databases and
/// have their pool connections closed automatically when the test succeeds or fails.
///
/// Takes an (async) test function as an argument and passes over the `TestDatabaseManager`
/// instance which can be used to build databases from inside the tests.
pub fn with_db_manager_teardown<F: AsyncTestFnWithManager + Send + Sync + 'static>(test: F) {
    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("with_db_teardown")
        .build()
        .expect("Could not build tokio Runtime for test");

    // Instantiate the database manager
    let db_manager = TestDatabaseManager::new();

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
