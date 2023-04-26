// SPDX-License-Identifier: AGPL-3.0-or-later

//! Build and manage a GraphQL schema including dynamic parts of the schema.
use std::sync::Arc;

use async_graphql::dynamic::{Object, Schema};
use async_graphql::{Request, Response};
use dynamic_graphql::internal::Registry;
use log::{debug, info};
use p2panda_rs::Human;
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::SqlStore;
use crate::graphql::input_values::{
    build_filter_input_object, build_order_enum_value, BooleanFilter, FloatFilter, IntegerFilter,
    MetaFilterInputObject, OrderDirection, PinnedRelationFilter, PinnedRelationListFilter,
    RelationFilter, RelationListFilter, StringFilter,
};
use crate::graphql::mutations::{MutationRoot, Publish};
use crate::graphql::objects::{build_document_object, build_paginated_document_object};
use crate::graphql::queries::{
    build_all_documents_query, build_document_query, build_next_args_query,
};
use crate::graphql::responses::NextArguments;
use crate::graphql::scalars::{
    CursorScalar, DocumentIdScalar, DocumentViewIdScalar, EncodedEntryScalar,
    EncodedOperationScalar, EntryHashScalar, LogIdScalar, PublicKeyScalar, SeqNumScalar,
};
/* use crate::graphql::types::{
    DocumentCollection, DocumentFields, DocumentMeta, DocumentSchema, OrderBy,
    OrderDirection, PaginatedDocumentSchema,
}; */
use crate::schema::SchemaProvider;

use super::objects::{
    build_document_collection_object, build_document_fields_object, DocumentMeta,
};

/// Returns GraphQL API schema for p2panda node.
///
/// Builds the root schema that can handle all GraphQL requests from clients (Client API) or other
/// nodes (Node API).
pub async fn build_root_schema(
    store: SqlStore,
    tx: ServiceSender,
    schema_provider: SchemaProvider,
) -> Schema {
    let all_schema = schema_provider.all().await;

    // Using dynamic-graphql we create a registry and add types
    let registry = Registry::new()
        // Register mutation operations
        .register::<MutationRoot>()
        .register::<Publish>()
        // Register responses
        .register::<NextArguments>()
        // Register objects
        .register::<DocumentMeta>()
        // Register input values
        .register::<BooleanFilter>()
        .register::<FloatFilter>()
        .register::<IntegerFilter>()
        .register::<MetaFilterInputObject>()
        .register::<OrderDirection>()
        .register::<PinnedRelationFilter>()
        .register::<PinnedRelationListFilter>()
        .register::<RelationFilter>()
        .register::<RelationListFilter>()
        .register::<StringFilter>()
        // Register scalars
        .register::<CursorScalar>()
        .register::<DocumentIdScalar>()
        .register::<DocumentViewIdScalar>()
        .register::<EncodedEntryScalar>()
        .register::<EncodedOperationScalar>()
        .register::<EntryHashScalar>()
        .register::<LogIdScalar>()
        .register::<PublicKeyScalar>()
        .register::<SeqNumScalar>();

    let mut schema_builder = Schema::build("Query", Some("MutationRoot"), None);

    // Populate it with the registered types. We can now use these in any following dynamically
    // created query object fields.
    schema_builder = registry.apply_into_schema_builder(schema_builder);

    // Construct the root query object
    let mut root_query = Object::new("Query");

    // Loop through all schema retrieved from the schema store, dynamically create GraphQL objects,
    // input values and a query for the documents they describe
    for schema in all_schema {
        // Construct the fields type object which will be named `<schema_id>Field`
        let document_fields_object = build_document_fields_object(&schema);

        // Construct the document object which contains "fields" and "meta" fields
        let document_object = build_document_object(&schema);

        // Construct the paginated response wrapper for this document schema type
        let document_collection_object = build_document_collection_object(&schema);

        // Construct the document object which contains "fields" and "meta" fields as well as
        // "cursor" pagination field
        let paginated_document_object = build_paginated_document_object(&schema);

        // Construct the filter and ordering input values for this schema
        let filter_input = build_filter_input_object(&schema);
        let order_input = build_order_enum_value(&schema);

        // Register a schema, schema fields and filter type for every schema
        schema_builder = schema_builder
            .register(document_fields_object)
            .register(document_object)
            .register(document_collection_object)
            .register(paginated_document_object)
            .register(order_input)
            .register(filter_input);

        // Add a query for each schema. It offers an interface to retrieve a single document of
        // this schema by it's document id or view id. Its resolver parses and validates the passed
        // parameters, then forwards them up to the children query fields
        root_query = build_document_query(root_query, &schema);

        // Add a query for retrieving all documents of a certain schema
        root_query = build_all_documents_query(root_query, &schema);
    }

    // Add next args to the query object
    let root_query = build_next_args_query(root_query);

    // Build the GraphQL schema. We can unwrap here since it will only fail if we forgot to
    // register all required types above
    schema_builder
        .register(root_query)
        .data(store)
        .data(schema_provider)
        .data(tx)
        .finish()
        .unwrap()
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
// an ever-growing list. See: https://github.com/p2panda/aquadoggo/issues/222
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

    use crate::test_utils::{add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
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
