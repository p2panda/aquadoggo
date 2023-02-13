// SPDX-License-Identifier: AGPL-3.0-or-later

//! Build and manage a GraphQL schema including dynamic parts of the schema.
use std::sync::Arc;

use async_graphql::{EmptySubscription, MergedObject, Request, Response, Schema};
use log::{debug, info};
use p2panda_rs::Human;
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::SqlStore;
use crate::graphql::client::{ClientMutationRoot, ClientRoot};
use crate::graphql::replication::ReplicationRoot;
use crate::schema::{save_static_schemas, SchemaProvider};

/// All of the GraphQL query sub modules merged into one top level root.
#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub ReplicationRoot, pub ClientRoot);

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
    store: SqlStore,
    tx: ServiceSender,
    schema_provider: SchemaProvider,
) -> RootSchema {
    // Configure query root
    let replication_root = ReplicationRoot::default();
    let client_query_root = ClientRoot::new();
    let query_root = QueryRoot(replication_root, client_query_root);

    // Configure mutation root
    let client_mutation_root = ClientMutationRoot::default();
    let mutation_root = MutationRoot(client_mutation_root);

    // Build GraphQL schema
    Schema::build(query_root, mutation_root, EmptySubscription)
        .data(store)
        .data(schema_provider)
        .data(tx)
        .finish()
}

/// Returns GraphQL API schema for p2panda node with a little trick to make dynamic schemas work.
///
/// The `async_graphql` crate we're using in this project does only provide methods to generate
/// GraphQL schemas statically. Ideally we would like to query our database for currently known
/// p2panda schemas and accordingly update the GraphQL schema whenever necessary but we don't have
/// static and sync access to the database when building `async_graphql` types.
///
/// With this little workaround we are still able to make it work! We load the p2panda schemas from
/// the database and write them into a temporary in-memory store. When `async_graphql` builds the
/// GraphQL schema we can load from this store statically to build the schemas on the fly.
async fn build_schema_with_workaround(shared: GraphQLSharedData) -> RootSchema {
    // Store all application schemas from database into static in-memory storage
    let all_schemas = shared.schema_provider.all().await;
    save_static_schemas(&all_schemas);

    // Build the actual GraphQL root schema, this will internally read the created JSON file and
    // accordingly build the schema
    build_root_schema(shared.store, shared.tx, shared.schema_provider)
}

/// List of created GraphQL root schemas.
type GraphQLSchemas = Arc<Mutex<Vec<RootSchema>>>;

/// Shared types between GraphQL schemas.
#[derive(Clone, Debug)]
pub struct GraphQLSharedData {
    /// Database interface.
    store: SqlStore,

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
//
// @TODO: This manager does not "clean up" outdated schemas yet, they will just be appended to
// an ever-growing list.
//
// WARNING: As soon as we start implementing GraphQL schema clean-up, we need to make sure to also
// free the used memory for all leaked schema data we've created. Otherwise this will lead to a
// memory leak! See `static_provider` module for more information (and useful tools) on this whole
// topic.
#[derive(Clone)]
pub struct GraphQLSchemaManager {
    /// List of all built GraphQL root schemas.
    schemas: GraphQLSchemas,

    /// Commonly shared types for GraphQL schemas.
    shared: GraphQLSharedData,
}

impl GraphQLSchemaManager {
    /// Returns a new instance of `GraphQLSchemaManager`.
    pub async fn new(store: SqlStore, tx: ServiceSender, schema_provider: SchemaProvider) -> Self {
        let schemas = Arc::new(Mutex::new(Vec::new()));
        let shared = GraphQLSharedData {
            store,
            tx,
            schema_provider,
        };

        // Create manager instance and spawn internal watch task
        let manager = Self { schemas, shared };
        manager.spawn_schema_added_task().await;

        manager
    }

    /// Subscribes to `SchemaProvider` for newly added schemas.
    ///
    /// This spawns a task which listens to new p2panda schemas to accordingly build a GraphQL
    /// schema which will be added to the list.
    async fn spawn_schema_added_task(&self) {
        let shared = self.shared.clone();
        let schemas = self.schemas.clone();

        info!("Subscribing GraphQL manager to schema provider");
        let mut on_schema_added = shared.schema_provider.on_schema_added();

        // Create the new GraphQL based on the current state of known p2panda application schemas
        async fn rebuild(shared: GraphQLSharedData, schemas: GraphQLSchemas) {
            let schema = build_schema_with_workaround(shared).await;
            schemas.lock().await.push(schema);
        }

        // Always build a schema right at the beginning as we don't have one yet
        rebuild(shared.clone(), schemas.clone()).await;
        debug!("Finished building initial GraphQL schema");

        // Spawn a task which reacts to newly registered p2panda schemas
        tokio::task::spawn(async move {
            loop {
                match on_schema_added.recv().await {
                    Ok(schema_id) => {
                        info!(
                            "Changed schema {}, rebuilding GraphQL API",
                            schema_id.display()
                        );
                        rebuild(shared.clone(), schemas.clone()).await;
                    }
                    Err(err) => {
                        panic!("Failed receiving schema updates: {}", err)
                    }
                }
            }
        });
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

#[cfg(test)]
mod test {
    use async_graphql::{value, Response};
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::constants::PRIVATE_KEY;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;
    use serde_json::{json, Value};
    use serial_test::serial;

    use crate::test_utils::{add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
    // Note: This test uses the underlying static schema provider which is a static mutable data
    // store, accessible across all test runner threads in parallel mode. To prevent overwriting
    // data across threads we have to run this test in serial.
    //
    // Read more: https://users.rust-lang.org/t/static-mutables-in-tests/49321
    #[serial]
    fn schema_updates() {
        test_runner(|mut node: TestNode| async move {
            // Create test client in the beginning so it is initialised with just the system
            // schemas. Then we create a new application schema to test that the graphql schema
            // is updated and we can query the changed schema.
            let client = graphql_test_client(&node).await;

            // This test uses a fixed private key to allow us to anticipate the schema typename.
            let key_pair = key_pair(PRIVATE_KEY);
            let type_name =
                "schema_name_0020f71816cb258a24bab65d63d42298467cc983670adc0bb2dd35e19f937011fbb2";

            // Check that the schema does not exist yet.
            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": format!(
                        r#"{{
                        schema: __type(name: "{}") {{
                            name,
                        }},
                    }}"#,
                        type_name,
                    ),
                }))
                .send()
                .await;
            let response: Response = response.json().await;

            assert_eq!(
                response.data,
                value!({
                    "schema": Value::Null,
                }),
                "\n{:#?}\n",
                response.errors
            );

            // Add schema to node.
            let schema = add_schema(
                &mut node,
                "schema_name",
                vec![("bool_field", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            assert_eq!(
                schema.id().to_string(),
                type_name,
                "Please update `type_name` const above to fix this test."
            );

            // Query gql schema.
            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": format!(
                        r#"{{
                        schema: __type(name: "{}") {{
                            name,
                        }},
                    }}"#,
                        type_name,
                    ),
                }))
                .send()
                .await;
            let response: Response = response.json().await;

            assert_eq!(
                response.data,
                value!({
                    "schema": {
                        "name": type_name
                    },
                }),
                "\n{:#?}\n",
                response.errors
            );
        });
    }
}
