// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::sync::Arc;

use futures::Future;
use log::{debug, error, info};
use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewId};
use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::{Author, KeyPair};
use p2panda_rs::operation::{
    AsOperation, AsVerifiedOperation, Operation, OperationEncoded, OperationFields, OperationId,
    OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList, VerifiedOperation,
};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};
use p2panda_rs::test_utils::constants::{PRIVATE_KEY, SCHEMA_ID};
use p2panda_rs::test_utils::fixtures::{operation, operation_fields};
use rstest::fixture;
use sqlx::migrate::MigrateDatabase;
use sqlx::Any;
use tokio::runtime::Builder;
use tokio::sync::Mutex;

use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::db::request::{EntryArgsRequest, PublishEntryRequest};
use crate::db::traits::DocumentStore;
use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};
use crate::graphql::client::NextEntryArgumentsType;
use crate::materializer::tasks::{dependency_task, reduce_task, schema_task};
use crate::materializer::TaskInput;
use crate::test_helpers::TEST_CONFIG;
use crate::{Configuration, SchemaProvider};

/// The fields used as defaults in the tests.
pub fn doggo_test_fields() -> Vec<(&'static str, OperationValue)> {
    vec![
        ("username", OperationValue::Text("bubu".to_owned())),
        ("height", OperationValue::Float(3.5)),
        ("age", OperationValue::Integer(28)),
        ("is_admin", OperationValue::Boolean(false)),
        (
            "profile_picture",
            OperationValue::Relation(Relation::new(
                Hash::new("0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    .unwrap()
                    .into(),
            )),
        ),
        (
            "special_profile_picture",
            OperationValue::PinnedRelation(PinnedRelation::new(
                Hash::new("0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .unwrap()
                    .into(),
            )),
        ),
        (
            "many_profile_pictures",
            OperationValue::RelationList(RelationList::new(vec![
                Hash::new("0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .unwrap()
                    .into(),
                Hash::new("0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                    .unwrap()
                    .into(),
            ])),
        ),
        (
            "many_special_profile_pictures",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
                    .unwrap()
                    .into(),
                Hash::new("0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                    .unwrap()
                    .into(),
            ])),
        ),
        (
            "another_relation_field",
            OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                Hash::new("0020abababababababababababababababababababababababababababababababab")
                    .unwrap()
                    .into(),
                Hash::new("0020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
                    .unwrap()
                    .into(),
            ])),
        ),
    ]
}

/// Helper for creating many key_pairs.
pub fn test_key_pairs(no_of_authors: usize) -> Vec<KeyPair> {
    let mut key_pairs = vec![KeyPair::from_private_key_str(PRIVATE_KEY).unwrap()];

    for _index in 1..no_of_authors {
        key_pairs.push(KeyPair::new())
    }

    key_pairs
}

/// Helper for constructing a publish entry request.
pub async fn construct_publish_entry_request(
    provider: &SqlStorage,
    operation: &Operation,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
) -> PublishEntryRequest {
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
    let entry_args_request = EntryArgsRequest {
        public_key: author.clone(),
        document_id: document_id.cloned(),
    };
    let next_entry_args = provider.get_entry_args(&entry_args_request).await.unwrap();

    let entry = Entry::new(
        &next_entry_args.log_id.into(),
        Some(operation),
        next_entry_args.skiplink.map(Hash::from).as_ref(),
        next_entry_args.backlink.map(Hash::from).as_ref(),
        &next_entry_args.seq_num.into(),
    )
    .unwrap();

    let entry = sign_and_encode(&entry, key_pair).unwrap();
    let operation = OperationEncoded::try_from(operation).unwrap();
    PublishEntryRequest { entry, operation }
}

/// Helper for inserting an entry, operation and document_view into the database.
pub async fn insert_entry_operation_and_view(
    provider: &SqlStorage,
    key_pair: &KeyPair,
    document_id: Option<&DocumentId>,
    operation: &Operation,
) -> (DocumentId, DocumentViewId) {
    if !operation.is_create() && document_id.is_none() {
        panic!("UPDATE and DELETE operations require a DocumentId to be passed")
    }

    let request = construct_publish_entry_request(provider, operation, key_pair, document_id).await;

    let operation_id: OperationId = request.entry.hash().into();
    let document_id = document_id
        .cloned()
        .unwrap_or_else(|| request.entry.hash().into());

    let document_view_id: DocumentViewId = request.entry.hash().into();

    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

    provider.publish_entry(&request).await.unwrap();
    provider
        .insert_operation(
            &VerifiedOperation::new(&author, &operation_id, operation).unwrap(),
            &document_id,
        )
        .await
        .unwrap();

    let document_operations = provider
        .get_operations_by_document_id(&document_id)
        .await
        .unwrap();

    let document = DocumentBuilder::new(document_operations).build().unwrap();

    provider.insert_document(&document).await.unwrap();

    (document_id, document_view_id)
}

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

pub struct PopulateDatabaseConfig {
    /// Number of entries per log/document.
    pub no_of_entries: usize,

    /// Number of logs for each author.
    pub no_of_logs: usize,

    /// Number of authors, each with logs populated as defined above.
    pub no_of_authors: usize,

    /// A boolean flag for wether all logs should contain a delete operation.
    pub with_delete: bool,

    /// The schema used for all operations in the db.
    pub schema: SchemaId,

    /// The fields used for every CREATE operation.
    pub create_operation_fields: Vec<(&'static str, OperationValue)>,

    /// The fields used for every UPDATE operation.
    pub update_operation_fields: Vec<(&'static str, OperationValue)>,
}

impl Default for PopulateDatabaseConfig {
    fn default() -> Self {
        Self {
            no_of_entries: 0,
            no_of_logs: 0,
            no_of_authors: 0,
            with_delete: false,
            schema: SCHEMA_ID.parse().unwrap(),
            create_operation_fields: doggo_test_fields(),
            update_operation_fields: doggo_test_fields(),
        }
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
            result.unwrap();
        });
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
        result.unwrap();
    });
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
    #[default(doggo_test_fields())] create_operation_fields: Vec<(&'static str, OperationValue)>,
    // The fields used for every UPDATE operation
    #[default(doggo_test_fields())] update_operation_fields: Vec<(&'static str, OperationValue)>,
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

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase {
    pub context: Context,
    pub store: SqlStorage,
    pub test_data: TestData,
}

impl TestDatabase {
    /// Publish a document and materialise it in the store.
    ///
    /// Also runs dependency task for document.
    pub async fn add_document(
        &mut self,
        schema_id: &SchemaId,
        fields: OperationFields,
        key_pair: &KeyPair,
    ) -> DocumentViewId {
        info!("Creating document for {}", schema_id);

        // Get requested schema from store.
        let schema = self
            .context
            .schema_provider
            .get(schema_id)
            .await
            .expect("Schema not found");

        // Build, publish and reduce create operation for document.
        let create_op = Operation::new_create(schema.id().to_owned(), fields).unwrap();
        let (entry_signed, _) = send_to_store(&self.store, &create_op, None, key_pair).await;
        let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
        let dependency_tasks = reduce_task(self.context.clone(), input.clone())
            .await
            .unwrap();

        // Run dependency tasks
        if let Some(tasks) = dependency_tasks {
            for task in tasks {
                dependency_task(self.context.clone(), task.input().to_owned())
                    .await
                    .unwrap();
            }
        }
        DocumentViewId::from(entry_signed.hash())
    }

    /// Publish a schema and materialise it in the store.
    pub async fn add_schema(
        &mut self,
        name: &str,
        fields: Vec<(&str, FieldType)>,
        key_pair: &KeyPair,
    ) -> Schema {
        info!("Creating schema {}", name);
        let mut field_ids = Vec::new();

        // Build and reduce schema field definitions
        for field in fields {
            let create_field_op = Schema::create_field(field.0, field.1.clone().into()).unwrap();
            let (entry_signed, _) =
                send_to_store(&self.store, &create_field_op, None, key_pair).await;

            let input = TaskInput::new(Some(DocumentId::from(entry_signed.hash())), None);
            reduce_task(self.context.clone(), input).await.unwrap();

            info!("Added field '{}' ({})", field.0, field.1.serialise());
            field_ids.push(DocumentViewId::from(entry_signed.hash()));
        }

        // Build and reduce schema definition
        let create_schema_op =
            Schema::create(name, "test schema description", field_ids.into()).unwrap();
        let (entry_signed, _) = send_to_store(&self.store, &create_schema_op, None, key_pair).await;
        let input = TaskInput::new(None, Some(DocumentViewId::from(entry_signed.hash())));
        reduce_task(self.context.clone(), input.clone())
            .await
            .unwrap();

        // Run schema task for this spec
        schema_task(self.context.clone(), input).await.unwrap();

        let view_id = DocumentViewId::from(entry_signed.hash());
        let schema_id = SchemaId::Application(name.to_string(), view_id);

        debug!("Done building {}", schema_id);
        self.context
            .schema_provider
            .get(&schema_id)
            .await
            .expect("Failed adding schema to provider.")
    }
}

/// Data collected when populating a `TestData` base in order to easily check values which
/// would be otherwise hard or impossible to get through the store methods.
#[derive(Default)]
pub struct TestData {
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}

/// Helper method for populating a `TestDatabase` with configurable data.
///
/// Passed parameters define what the db should contain. The first entry in each log contains a
/// valid CREATE operation following entries contain duplicate UPDATE operations. If the
/// with_delete flag is set to true the last entry in all logs contain be a DELETE operation.
pub async fn populate_test_db(db: &mut TestDatabase, config: &PopulateDatabaseConfig) {
    let key_pairs = test_key_pairs(config.no_of_authors);

    for key_pair in &key_pairs {
        db.test_data
            .key_pairs
            .push(KeyPair::from_private_key(key_pair.private_key()).unwrap());

        for _log_id in 0..config.no_of_logs {
            let mut document_id: Option<DocumentId> = None;
            let mut previous_operation: Option<DocumentViewId> = None;

            for index in 0..config.no_of_entries {
                // Create an operation based on the current index and whether this document should
                // contain a DELETE operation
                let next_operation_fields = match index {
                    // First operation is CREATE
                    0 => Some(operation_fields(config.create_operation_fields.clone())),
                    // Last operation is DELETE if the with_delete flag is set
                    seq if seq == (config.no_of_entries - 1) && config.with_delete => None,
                    // All other operations are UPDATE
                    _ => Some(operation_fields(config.update_operation_fields.clone())),
                };

                // Publish the operation encoded on an entry to storage.
                let (entry_encoded, publish_entry_response) = send_to_store(
                    &db.store,
                    &operation(
                        next_operation_fields,
                        previous_operation,
                        Some(config.schema.to_owned()),
                    ),
                    document_id.as_ref(),
                    key_pair,
                )
                .await;

                // Set the previous_operations based on the backlink
                previous_operation = publish_entry_response.backlink.map(DocumentViewId::from);

                // If this was the first entry in the document, store the doucment id for later.
                if index == 0 {
                    document_id = Some(entry_encoded.hash().into());
                    db.test_data.documents.push(document_id.clone().unwrap());
                }
            }
        }
    }
}

/// Helper method for publishing an operation encoded on an entry to a store.
pub async fn send_to_store(
    store: &SqlStorage,
    operation: &Operation,
    document_id: Option<&DocumentId>,
    key_pair: &KeyPair,
) -> (EntrySigned, NextEntryArgumentsType) {
    // Get an Author from the key_pair.
    let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

    // Get the next entry arguments for this author and the passed document id.
    let next_entry_args = store
        .get_entry_args(&EntryArgsRequest {
            public_key: author.clone(),
            document_id: document_id.cloned(),
        })
        .await
        .unwrap();

    // Construct the next entry.
    let next_entry = Entry::new(
        &next_entry_args.log_id.into(),
        Some(operation),
        next_entry_args.skiplink.map(Hash::from).as_ref(),
        next_entry_args.backlink.map(Hash::from).as_ref(),
        &next_entry_args.seq_num.into(),
    )
    .unwrap();

    // Encode both the entry and operation.
    let entry_encoded = sign_and_encode(&next_entry, key_pair).unwrap();
    let operation_encoded = OperationEncoded::try_from(operation).unwrap();

    // Publish the entry and get the next entry args.
    let publish_entry_request = PublishEntryRequest {
        entry: entry_encoded.clone(),
        operation: operation_encoded,
    };
    let publish_entry_response = store.publish_entry(&publish_entry_request).await.unwrap();

    // Set or unwrap the passed document_id.
    let document_id = if operation.is_create() {
        entry_encoded.hash().into()
    } else {
        document_id.unwrap().to_owned()
    };

    // Also insert the operation into the store.
    let verified_operation =
        VerifiedOperation::new(&author, &entry_encoded.hash().into(), operation).unwrap();
    match store
        .insert_operation(&verified_operation, &document_id)
        .await
    {
        Ok(_) => {}
        Err(err) => error!("Failed inserting operation: {}", err),
    }

    (entry_encoded, publish_entry_response)
}

/// Create test database.
async fn initialize_db() -> Pool {
    // Reset database first
    drop_database().await;
    create_database(&TEST_CONFIG.database_url).await.unwrap();

    // Create connection pool and run all migrations
    let pool = connection_pool(&TEST_CONFIG.database_url, 25)
        .await
        .unwrap();
    if run_pending_migrations(&pool).await.is_err() {
        pool.close().await;
    }

    pool
}

/// Create test database.
async fn initialize_db_with_url(url: &str) -> Pool {
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
async fn drop_database() {
    if Any::database_exists(&TEST_CONFIG.database_url)
        .await
        .unwrap()
    {
        Any::drop_database(&TEST_CONFIG.database_url).await.unwrap();
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

        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        let test_db = TestDatabase {
            context,
            store,
            test_data: TestData::default(),
        };
        self.pools.lock().await.push(pool);
        test_db
    }
}
