// SPDX-License-Identifier: AGPL-3.0-or-later

//! Build and manage a GraphQL schema including dynamic parts of the schema.
use std::sync::Arc;

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Interface, Object, Schema, TypeRef};
use async_graphql::indexmap::IndexMap;
use async_graphql::{Error, Name, Request, Response, Value};
use dynamic_graphql::internal::Registry;
use dynamic_graphql::{FieldValue, ScalarValue};
use log::{debug, info};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::FieldType;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::Human;
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::SqlStore;
use crate::dynamic_graphql::scalars::{
    DocumentIdScalar, DocumentViewIdScalar, EncodedEntryScalar, EncodedOperationScalar,
    EntryHashScalar, LogIdScalar, PublicKeyScalar, SeqNumScalar,
};
use crate::dynamic_graphql::types::{DocumentMeta, NextArguments};
use crate::schema::SchemaProvider;

// Correctly formats the name of a document field type.
fn fields_name(name: &str) -> String {
    format!("{name}Fields")
}

/// Convert non-relation operation values into GraphQL values.
///
/// Panics when given a relation field value.
fn gql_scalar(operation_value: &OperationValue) -> Value {
    match operation_value {
        OperationValue::Boolean(value) => value.to_owned().into(),
        OperationValue::Integer(value) => value.to_owned().into(),
        OperationValue::Float(value) => value.to_owned().into(),
        OperationValue::String(value) => value.to_owned().into(),
        // only use for scalars
        _ => panic!("can only return scalar values"),
    }
}

/// Get the GraphQL type name for a p2panda field type.
///
/// GraphQL types for relations use the p2panda schema id as their name.
fn graphql_type(field_type: &FieldType) -> TypeRef {
    match field_type {
        p2panda_rs::schema::FieldType::Boolean => TypeRef::named_nn(TypeRef::BOOLEAN),
        p2panda_rs::schema::FieldType::Integer => TypeRef::named_nn(TypeRef::INT),
        p2panda_rs::schema::FieldType::Float => TypeRef::named_nn(TypeRef::FLOAT),
        p2panda_rs::schema::FieldType::String => TypeRef::named_nn(TypeRef::STRING),
        p2panda_rs::schema::FieldType::Relation(schema_id) => TypeRef::named(schema_id.to_string()),
        p2panda_rs::schema::FieldType::RelationList(schema_id) => {
            TypeRef::named_list(schema_id.to_string())
        }
        p2panda_rs::schema::FieldType::PinnedRelation(schema_id) => {
            TypeRef::named(schema_id.to_string())
        }
        p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
            TypeRef::named_list(schema_id.to_string())
        }
    }
}

/// Returns GraphQL API schema for p2panda node.
///
/// Builds the root schema that can handle all GraphQL requests from clients (Client API) or other
/// nodes (Node API).
pub async fn build_root_schema(
    store: SqlStore,
    tx: ServiceSender,
    schema_provider: SchemaProvider,
) -> Schema {
    // Get all schema from the schema provider.
    let all_schema = schema_provider.all().await;

    // Using dynamic-graphql we create a registry where we can add types.
    let registry = Registry::new()
        .register::<NextArguments>()
        .register::<DocumentIdScalar>()
        .register::<DocumentMeta>()
        .register::<DocumentViewIdScalar>()
        .register::<EncodedEntryScalar>()
        .register::<EncodedOperationScalar>()
        .register::<EntryHashScalar>()
        .register::<LogIdScalar>()
        .register::<PublicKeyScalar>()
        .register::<SeqNumScalar>();

    // Construct the schema builder.
    let mut schema_builder = Schema::build("Query", None, None);

    // Populate it with the registered types. We can now use these in any following dynamically
    // created query object fields.
    schema_builder = registry.apply_into_schema_builder(schema_builder);

    // Construct the root query object.
    let mut query = Object::new("Query");

    // Loop through all schema retrieved from the schema store and add types and query for the
    // documents they describe.
    for schema in all_schema {
        // Construct the document fields type.
        let document_fields_name = fields_name(&schema.id().to_string());
        let mut document_fields = Object::new(&document_fields_name);

        for (name, field_type) in schema.fields() {
            let graphql_type = graphql_type(field_type);

            document_fields = document_fields.field(Field::new(name, graphql_type, move |ctx| {
                FieldFuture::new(async move {
                    let field_name = ctx.field().name();
                    match ctx.parent_value.as_value() {
                        Some(Value::Object(map)) => {
                            let value = map
                                .get(field_name)
                                .map(|value| FieldValue::value(value.to_owned()));
                            Ok(value)
                        }
                        _ => Ok(FieldValue::NONE),
                    }
                })
            }))
        }

        // Construct the document type.
        let document = Object::new(&schema.id().to_string())
            .field(Field::new(
                "fields",
                TypeRef::named_nn(&document_fields_name),
                move |ctx| {
                    FieldFuture::new(async move {
                        let field_name = ctx.field().name();
                        match ctx.parent_value.as_value() {
                            Some(Value::Object(map)) => {
                                let value = map
                                    .get(field_name)
                                    .map(|value| FieldValue::value(value.to_owned()));
                                Ok(value)
                            }
                            _ => Ok(FieldValue::NONE),
                        }
                    })
                },
            ))
            .field(Field::new(
                "meta",
                TypeRef::named_nn("DocumentMeta"),
                move |ctx| {
                    FieldFuture::new(async move {
                        let field_name = ctx.field().name();
                        match ctx.parent_value.as_value() {
                            Some(Value::Object(map)) => match map.get(field_name).unwrap() {
                                Value::Object(document_meta) => {
                                    let document_meta = DocumentMeta {
                                        document_id: DocumentIdScalar::from_value(
                                            document_meta.get("document_id").unwrap().to_owned(),
                                        )
                                        .unwrap(),
                                        view_id: DocumentViewIdScalar::from_value(
                                            document_meta.get("view_id").unwrap().to_owned(),
                                        )
                                        .unwrap(),
                                    };
                                    Ok(Some(FieldValue::owned_any(document_meta)))
                                }
                                _ => Ok(FieldValue::NONE),
                            },
                            _ => Ok(FieldValue::NONE),
                        }
                    })
                },
            ))
            .description(schema.description());

        // Register a document and document fields type for every schema.
        schema_builder = schema_builder.register(document_fields).register(document);

        // Add a query object for each schema.
        query = query.field(
            Field::new(
                schema.id().to_string(),
                TypeRef::named(schema.id().to_string()),
                move |ctx| {
                    FieldFuture::new(async move {
                        let store = ctx.data_unchecked::<SqlStore>();
                        let document_id = ctx.args.get("id");
                        let document_view_id = ctx.args.get("view_id");
                        let document = match (document_id, document_view_id) {
                            (Some(document_id), None) => {
                                let id: DocumentId = document_id.deserialize()?;
                                store.get_document(&id).await?
                            }
                            (None, None) => return Err(Error::new(
                                "Either document id or document view id arguments must be passed",
                            )),
                            (None, Some(document_view_id)) => {
                                let document_view_id_str = document_view_id.string()?;
                                let document_view_id: DocumentViewId = document_view_id_str.parse()?;

                                store.get_document_by_view_id(&document_view_id).await?
                            }
                            (Some(_), Some(_)) => return Err(Error::new(
                                "Both document id and document view id arguments cannot be passed",
                            )),
                        };
                        match document {
                            Some(document) => {
                                if document.is_deleted() {
                                    return Ok(FieldValue::NONE);
                                };

                                let fields = document.fields().expect(
                                    "All documents which haven't been deleted should have fields",
                                );

                                let mut fields_value = IndexMap::new();
                                for (name, value) in fields.iter() {
                                    fields_value.insert(Name::new(name), gql_scalar(value.value()));
                                }

                                let mut document_meta_value = IndexMap::new();
                                document_meta_value.insert(
                                    Name::new("document_id"),
                                    Value::String(document.id().to_string()),
                                );
                                document_meta_value.insert(
                                    Name::new("view_id"),
                                    Value::String(document.view_id().to_string()),
                                );

                                let mut document_value = IndexMap::new();
                                document_value
                                    .insert(Name::new("fields"), Value::Object(fields_value));
                                document_value
                                    .insert(Name::new("meta"), Value::Object(document_meta_value));
                                let document_value =
                                    FieldValue::value(Value::Object(document_value));
                                Ok(Some(document_value))
                            }
                            None => Ok(FieldValue::NONE),
                        }
                    })
                },
            )
            .argument(InputValue::new("id", TypeRef::named("DocumentId")))
            .argument(InputValue::new("view_id", TypeRef::named("DocumentViewId")))
            .description(format!(
                "Query a {} document by id or view id",
                schema.name()
            )),
        )
    }

    // Add next args to the query object.
    let query = query.field(
        Field::new("nextArgs", TypeRef::named("NextArguments"), |ctx| {
            FieldFuture::new(async move {
                let _public_key: PublicKey = ctx.args.get("publicKey").unwrap().deserialize()?;
                let _document_view_id: Option<DocumentViewId> =
                    ctx.args.get("documentViewId").unwrap().deserialize()?;
                Ok(Some(FieldValue::owned_any(NextArguments {
                    log_id: "0".to_string(),
                    seq_num: "1".to_string(),
                    backlink: None,
                    skiplink: None,
                })))
            })
        })
        .argument(InputValue::new("publicKey", TypeRef::named_nn("PublicKey")))
        .argument(InputValue::new(
            "documentViewId",
            TypeRef::named("DocumentViewId"),
        ))
        .description("Gimme some sweet sweet next args!"),
    );

    // Build the schema.
    schema_builder
        .register(query)
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
