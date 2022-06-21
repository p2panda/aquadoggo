// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use async_graphql::{EmptySubscription, MergedObject, Request, Response, Schema};
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::provider::SqlStorage;
use crate::graphql::client::{ClientMutationRoot, ClientRoot};
use crate::graphql::replication::context::ReplicationContext;
use crate::graphql::replication::ReplicationRoot;
use crate::schema_service::{SchemaService, TempFile};

pub const TEMP_FILE_PATH: &'static str = "./aquadoggo-schemas.temp";

/// All of the graphql query sub modules merged into one top level root.
#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub ReplicationRoot<SqlStorage>, pub ClientRoot);

/// All of the graphql mutation sub modules merged into one top level root.
#[derive(MergedObject, Debug, Copy, Clone, Default)]
pub struct MutationRoot(pub ClientMutationRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// GraphQL schema for p2panda node.
///
/// Build the root graphql schema that can handle graphql requests.
pub fn build_root_schema(
    store: SqlStorage,
    tx: ServiceSender,
    schema_service: SchemaService,
) -> RootSchema {
    let replication_context = Arc::new(Mutex::new(ReplicationContext::new(1000, store.clone())));

    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let client_query_root = ClientRoot::new(store.clone(), schema_service);
    let query_root = QueryRoot(replication_root, client_query_root);

    let client_mutation_root = ClientMutationRoot::default();
    let mutation_root = MutationRoot(client_mutation_root);

    Schema::build(query_root, mutation_root, EmptySubscription)
        .data(replication_context)
        .data(store)
        .data(tx)
        .finish()
}

#[derive(Clone)]
pub struct GraphQLSharedData {
    store: SqlStorage,
    tx: ServiceSender,
    schema_service: SchemaService,
}

#[derive(Clone)]
pub struct GraphQLSchemaManager {
    schemas: Arc<Mutex<Vec<RootSchema>>>,
    shared: GraphQLSharedData,
}

impl GraphQLSchemaManager {
    pub fn new(store: SqlStorage, tx: ServiceSender, schema_service: SchemaService) -> Self {
        let schema = build_root_schema(store.clone(), tx.clone(), schema_service.clone());

        let schemas = Arc::new(Mutex::new(vec![schema]));
        let shared = GraphQLSharedData {
            store,
            tx,
            schema_service,
        };

        Self { schemas, shared }
    }

    pub async fn execute(&self, request: impl Into<Request>) -> Response {
        self.schemas
            .lock()
            .await
            .last()
            .expect("No schema given yet")
            .execute(request)
            .await
    }

    pub async fn build_root_schema(&mut self) {
        // Store temporary file with all schemas inside on file-system.
        //
        // This is a workaround `async_graphql` not being able to introduce new schemas during
        // runtime: We don't get access to our database from within defining GraphQL schemas but
        // only to the file system.
        //
        // @TODO: We could try to still access the database by creating a static interface to it?
        let all_schemas = self
            .shared
            .schema_service
            .all_schemas()
            .await
            .expect("Loading all schemas from database failed");
        let temp_file = TempFile::save(&all_schemas, TEMP_FILE_PATH);

        println!("BUILD ROOT SCHEMA");

        // Create the new GraphQL based on the current state of known p2panda application schemas
        let schema = build_root_schema(
            self.shared.store.clone(),
            self.shared.tx.clone(),
            self.shared.schema_service.clone(),
        );
        self.schemas.lock().await.push(schema);

        // Remove temporary file after schema got created
        temp_file.unlink();
    }
}
