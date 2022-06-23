// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;
use std::sync::Arc;

use async_graphql::{EmptySubscription, MergedObject, Request, Response, Schema};
use log::{error, info, warn};
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::provider::SqlStorage;
use crate::graphql::client::{ClientMutationRoot, ClientRoot};
use crate::graphql::error::DynamicSchemaError;
use crate::graphql::replication::context::ReplicationContext;
use crate::graphql::replication::ReplicationRoot;
use crate::graphql::TempFile;
use crate::schema::SchemaProvider;

/// All of the GraphQL query sub modules merged into one top level root.
#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub ReplicationRoot<SqlStorage>, pub ClientRoot);

/// All of the GraphQL mutation sub modules merged into one top level root.
#[derive(MergedObject, Debug, Copy, Clone, Default)]
pub struct MutationRoot(pub ClientMutationRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Returns GraphQL API schema for p2panda node.
///
/// Builds the root schema that can handle all GraphQL requests from clients (Client API) or other
/// nodes (Node API).
pub fn build_root_schema(
    store: SqlStorage,
    tx: ServiceSender,
    schema_provider: SchemaProvider,
) -> RootSchema {
    let replication_context = Arc::new(Mutex::new(ReplicationContext::new(1000, store.clone())));

    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let client_query_root = ClientRoot::new(schema_provider);
    let query_root = QueryRoot(replication_root, client_query_root);

    let client_mutation_root = ClientMutationRoot::default();
    let mutation_root = MutationRoot(client_mutation_root);

    Schema::build(query_root, mutation_root, EmptySubscription)
        .data(replication_context)
        .data(store)
        .data(tx)
        .finish()
}

/// Location of the temporary file containing all currently known schemas.
pub const TEMP_FILE_FNAME: &str = ".schemas.tmp.json";

/// Returns GraphQL API schema for p2panda node with a little trick to make dynamic schemas work.
///
/// The `async_graphql` crate we're using in this project does only provide methods to generate
/// GraphQL schemas statically. Ideally we would like to query our database for currently known
/// p2panda schemas and accordingly update the GraphQL schema whenever necessary but we don't have
/// static access to the database when building `async_graphql` types.
///
/// With this little workaround we are still able to make it work! We load the p2panda schemas from
/// the database and serialise them into a JSON file. When `async_graphql` builds the GraphQL
/// schema we can load this file statically to build the schemas dynamically based on the file's
/// content.
async fn build_schema_with_workaround(
    shared: GraphQLSharedData,
) -> Result<RootSchema, DynamicSchemaError> {
    // Store temporary JSON file with all serialised application schemas from database.
    //
    // @TODO: We could try to still access the database by creating a static interface to it?
    let all_schemas = shared.schema_provider.all();
    let temp_file = TempFile::save(&all_schemas, TEMP_FILE_FNAME)?;

    // Build the actual GraphQL root schema, this will internally read the created JSON file and
    // accordingly build the schema
    let schema = build_root_schema(shared.store, shared.tx, shared.schema_provider);

    // Remove temporary file as we don't need it anymore
    temp_file.unlink();

    Ok(schema)
}

/// List of created GraphQL root schemas.
type GraphQLSchemas = Arc<Mutex<Vec<RootSchema>>>;

/// Shared types between GraphQL schemas.
#[derive(Clone, Debug)]
pub struct GraphQLSharedData {
    /// Database interface.
    store: SqlStorage,

    /// Communication bus interface to send messages to other services.
    tx: ServiceSender,

    /// Schema provider giving us access to currently known schemas.
    schema_provider: SchemaProvider,
}

/// Builds new GraphQL schemas dynamically and executes the latest GraphQL schema for incoming
/// queries.
///
/// This manager allows us to introduce new GraphQL schemas during runtime as it internally handles
/// a list of schemas (behind a mutex) and automatically picks the "latest" as soon as a query
/// needs to be executed.
///
/// With this we can easily add "new" schemas to the list in the background while current queries
/// still get processed using the "old" schema.
#[derive(Clone)]
pub struct GraphQLSchemaManager {
    /// List of all built GraphQL root schemas.
    schemas: GraphQLSchemas,

    /// Commonly shared types for GraphQL schemas.
    shared: GraphQLSharedData,
}

impl GraphQLSchemaManager {
    /// Returns a new instance of `GraphQLSchemaManager`.
    pub async fn new(
        store: SqlStorage,
        tx: ServiceSender,
        schema_provider: SchemaProvider,
    ) -> Self {
        let schemas = Arc::new(Mutex::new(Vec::new()));
        let shared = GraphQLSharedData {
            store,
            tx,
            schema_provider,
        };

        // Create manager instance and spawn internal watch task
        let manager = Self { schemas, shared };
        manager
            .spawn_schema_added_task()
            .await
            .map_err(|err| panic!("Failed building initial schema: {}", err))
            .unwrap();

        manager
    }

    /// Subscribes to `SchemaProvider` for newly added schemas.
    ///
    /// This spawns a task which listens to new p2panda schemas to accordingly build a GraphQL
    /// schema which will be added to the list.
    // @TODO: This manager does not "clean up" outdated schemas yet, they will just be appended to
    // an ever-growing list.
    async fn spawn_schema_added_task(&self) -> Result<(), DynamicSchemaError> {
        let shared = self.shared.clone();
        let schemas = self.schemas.clone();

        let mut on_schema_added = shared.schema_provider.on_schema_added();

        // Create the new GraphQL based on the current state of known p2panda application schemas
        async fn rebuild(
            shared: GraphQLSharedData,
            schemas: GraphQLSchemas,
        ) -> Result<(), DynamicSchemaError> {
            let schema = build_schema_with_workaround(shared).await?;
            schemas.lock().await.push(schema);
            Ok(())
        }

        // Always build a schema right at the beginning as we don't have one yet
        rebuild(shared.clone(), schemas.clone()).await?;

        // Spawn a task which reacts to newly registered p2panda schemas
        tokio::task::spawn(async move {
            loop {
                match on_schema_added.recv().await {
                    Ok(schema_id) => {
                        info!("Changed schema {}, rebuilding GraphQL API", schema_id);
                        if let Err(err) = rebuild(shared.clone(), schemas.clone()).await {
                            error!("Error updating graphql schema: {}", err);
                        }
                    }
                    Err(err) => {
                        panic!("Failed receiving schema updates: {}", err)
                    }
                }
            }
        });

        Ok(())
    }

    /// Executes an incoming GraphQL query.
    ///
    /// This method makes sure the GraphQL query will be executed by the latest given schema the
    /// manager knows about.
    pub async fn execute(&self, request: impl Into<Request>) -> Response {
        self.schemas
            .lock()
            .await
            .last()
            .expect("No schema given yet")
            .execute(request)
            .await
    }
}

impl std::fmt::Debug for GraphQLSchemaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `schemas` does not implement `Debug` but we can at least print the other fields
        f.debug_struct("GraphQLSchemaManager")
            .field("shared", &self.shared)
            .finish()
    }
}
