// SPDX-License-Identifier: AGPL-3.0-or-later

//! Build and manage a GraphQL schema including dynamic parts of the schema.
use std::sync::Arc;

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, Schema, TypeRef};
use async_graphql::{Number, Request, Response, Result, Value};
use dynamic_graphql::internal::Registry;
use dynamic_graphql::FieldValue;
use log::{debug, info};
use once_cell::sync::Lazy;
use p2panda_rs::Human;
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::SqlStore;
use crate::graphql::client::dynamic_types::NextArguments;
use crate::schema::SchemaProvider;

/// Some dummy values to return from queries.
const VALUES: Lazy<Vec<Value>> = Lazy::new(|| {
    vec![
        Value::String("boop".to_owned()),
        Value::String("is it me you're looking for?".to_owned()),
        Value::Number(Number::from(2)),
    ]
});

/// Returns GraphQL API schema for p2panda node.
///
/// Builds the root schema that can handle all GraphQL requests from clients (Client API) or other
/// nodes (Node API).
pub async fn build_root_schema(
    _store: SqlStore,
    _tx: ServiceSender,
    _schema_provider: SchemaProvider,
) -> Schema {
    // Query fields we want to dynamically add to the root query object.
    let query_fields = vec![
        ("beep", TypeRef::STRING, "The beep of the boop"),
        ("hello", TypeRef::STRING, "The song lyric, not the planet"),
        ("one", TypeRef::INT, "What comes after it?"),
    ];

    // Using dynamic-graphql we create a registry where we can add types.
    let registry = Registry::new().register::<NextArguments>();

    // Construct the schema builder.
    let schema = Schema::build("Query", None, None);

    // Populate it with the registered types. We can now use these in any following dynamically
    // created query object fields.
    let schema = registry.apply_into_schema_builder(schema);

    // Construct the root query object.
    let mut query = Object::new("Query");

    // Iterate over our query fields and insert them into the root query.
    for (index, field) in query_fields.into_iter().enumerate() {
        query = query.field(
            Field::new(field.0, TypeRef::named_nn(field.1), move |_ctx| {
                FieldFuture::new(async move { Ok(Some(VALUES[index].clone())) })
            })
            .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)))
            .description(field.2),
        )
    }

    // Add next args to the query object.
    let query = query.field(
        Field::new("nextArgs", TypeRef::named("NextArguments"), |_ctx| {
            FieldFuture::new(async move {
                Ok(Some(FieldValue::owned_any(NextArguments {
                    log_id: "0".to_string(),
                    seq_num: "1".to_string(),
                    backlink: None,
                    skiplink: None,
                })))
            })
        })
        .argument(InputValue::new(
            "publicKey",
            TypeRef::named_nn(TypeRef::STRING),
        ))
        .argument(InputValue::new(
            "documentViewId",
            TypeRef::named(TypeRef::STRING),
        ))
        .description("Gimme some sweet sweet next args!"),
    );

    // Build the schema.
    schema.register(query).finish().unwrap()
}

/// List of created GraphQL root schemas.
type GraphQLSchemas = Arc<Mutex<Vec<Schema>>>;

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
            let schema = build_root_schema(shared.store, shared.tx, shared.schema_provider).await;
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
