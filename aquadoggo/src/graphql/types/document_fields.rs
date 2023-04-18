// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, ResolverContext};
use async_graphql::Error;
use dynamic_graphql::FieldValue;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema};
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::stores::RelationList;
use crate::db::SqlStore;
use crate::graphql::types::DocumentValue;
use crate::graphql::utils::{
    downcast_document, fields_name, gql_scalar, graphql_type, with_collection_arguments,
};
use crate::schema::SchemaProvider;

use super::DocumentCollection;

/// A constructor for dynamically building objects describing the application fields of a p2panda
/// schema. Each generated object has a type name with the formatting `<schema_id>Fields`.
///
/// A type should be added to the root GraphQL schema for every schema supported on a node, as
/// these types are not known at compile time we make use of the `async-graphql` `dynamic` module.
pub struct DocumentFields;

impl DocumentFields {
    /// Build the fields object from the related p2panda schema.
    ///
    /// Constructs an object which can then be added to the root GraphQL schema.
    pub fn build(schema: &Schema) -> Object {
        // Construct the document fields object which will be named `<schema_id>Fields`
        let schema_field_name = fields_name(schema.id());
        let mut document_schema_fields = Object::new(&schema_field_name);

        // For every field in the schema we create a type with a resolver
        for (name, field_type) in schema.fields().iter() {
            // If this is a relation list type we add an argument for filtering items in the list
            let field = match field_type {
                p2panda_rs::schema::FieldType::RelationList(schema_id)
                | p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
                    with_collection_arguments(
                        Field::new(name, graphql_type(field_type), move |ctx| {
                            FieldFuture::new(async move { Self::resolve(ctx).await })
                        }),
                        schema_id,
                    )
                }
                _ => Field::new(name, graphql_type(field_type), move |ctx| {
                    FieldFuture::new(async move { Self::resolve(ctx).await })
                })
                .description(format!(
                    "The `{}` field of a {} document.",
                    name,
                    schema.id().name()
                )),
            };
            document_schema_fields = document_schema_fields.field(field).description(format!(
                "The application fields of a `{}` document.",
                schema.id().name()
            ));
        }

        document_schema_fields
    }

    /// Resolve a document field value as a graphql `FieldValue`. If the value is a relation, then
    /// the relevant document id or document view id is determined and passed along the query
    /// chain. If the value is a simple type (meaning it is also a query leaf) then it is directly
    /// resolved.
    ///
    /// Requires a `ResolverContext` to be passed into the method.
    async fn resolve(ctx: ResolverContext<'_>) -> Result<Option<FieldValue<'_>>, Error> {
        let store = ctx.data_unchecked::<SqlStore>();
        let schema_provider = ctx.data_unchecked::<SchemaProvider>();

        let name = ctx.field().name();

        // Parse the bubble up value
        let document = match downcast_document(&ctx) {
            DocumentValue::Single(document) => document,
            DocumentValue::Item(_, document) => document,
            DocumentValue::Collection(_, _) => panic!("Expected list item or single document"),
        };

        let schema = schema_provider
            .get(document.schema_id())
            .await
            .expect("Schema should be in store");

        // Get the field this query is concerned with
        match document.get(name).unwrap() {
            // Relation fields are expected to resolve to the related document so we pass along the
            // document id which will be processed through it's own resolver
            OperationValue::Relation(relation) => {
                // Get the whole document from the store.
                let document = match store.get_document(relation.document_id()).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

                let document = DocumentValue::Single(document);
                Ok(Some(FieldValue::owned_any(document)))
            }
            // Relation lists are handled by collecting and returning a list of all document ids in
            // the relation list
            OperationValue::RelationList(_) => {
                // Get the schema of documents in this relation list
                let relation_field_schema = schema
                    .fields()
                    .get(name)
                    .expect("Document field should exist on schema");

                // Get the schema itself
                let schema = match relation_field_schema {
                    FieldType::RelationList(schema_id) => {
                        // We can unwrap here as the schema should exist in the store already
                        schema_provider.get(schema_id).await.unwrap()
                    }
                    _ => panic!("Schema should exist"),
                };

                // Select relation field containing list of documents
                let list = RelationList::new_unpinned(document.view_id(), name);

                DocumentCollection::resolve(ctx, schema, Some(list)).await
            }
            // Pinned relation behaves the same as relation but passes along a document view id.
            OperationValue::PinnedRelation(relation) => {
                // Get the whole document from the store.
                let document = match store.get_document_by_view_id(relation.view_id()).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

                let document = DocumentValue::Single(document);
                Ok(Some(FieldValue::owned_any(document)))
            }
            // Pinned relation lists behave the same as relation lists but pass along view ids.
            OperationValue::PinnedRelationList(_) => {
                // Get the schema of documents in this relation list
                let relation_field_schema = schema
                    .fields()
                    .get(name)
                    .expect("Document field should exist on schema");

                // Get the schema itself
                let schema = match relation_field_schema {
                    FieldType::PinnedRelationList(schema_id) => {
                        // We can unwrap here as the schema should exist in the store
                        // already
                        schema_provider.get(schema_id).await.unwrap()
                    }
                    _ => panic!(), // Should never reach here.
                };

                // Select relation field containing list of pinned document views
                let list = RelationList::new_pinned(document.view_id(), name);

                DocumentCollection::resolve(ctx, schema, Some(list)).await
            }
            // All other fields are simply resolved to their scalar value.
            value => Ok(Some(FieldValue::value(gql_scalar(value)))),
        }
    }
}

// TODO: We don't actually perform any queries yet, these tests will need to be updated when we do.
// See issue: https://github.com/p2panda/aquadoggo/issues/330
/* #[cfg(test)]
mod test {
    use async_graphql::{value, Response, Value};
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{graphql_test_client, test_runner, TestNode};

    #[rstest]
    #[case(
        r#"(
            first: 10,
            after: "1_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331",
            orderBy: "name",
            orderDirection: ASC,
            filter: {
                name : {
                    eq: "hello"
                }
            },
        )"#.to_string(),
        value!({
            "collection": value!([]),
        }),
        vec![]
    )]
    fn collection_query(
        #[case] query_args: String,
        #[case] expected_data: Value,
        #[case] _expected_errors: Vec<String>,
    ) {
        // Test collection query parameter variations.
        test_runner(move |node: TestNode| async move {
            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                    collection: all_schema_definition_v1 {{
                        hasNextPage
                        totalCount
                        documents {{
                            cursor
                            fields {{
                                fields{query_args} {{
                                    documents {{
                                        cursor
                                    }}
                                }}
                            }}
                        }}
                    }},
                }}"#,
                query_args = query_args
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            assert_eq!(response.data, expected_data, "{:#?}", response.errors);
        });
    }
} */
