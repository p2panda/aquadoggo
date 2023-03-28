// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::Error;
use dynamic_graphql::FieldValue;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema};
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::query::{Field as FilterField, Filter, MetaField, Order, Pagination};
use crate::db::SqlStore;
use crate::graphql::scalars::CursorScalar;
use crate::graphql::utils::{
    downcast_document, fields_name, filter_name, gql_scalar, graphql_type, order_by_name,
    parse_collection_arguments,
};

use crate::graphql::types::{DocumentValue, PaginationData};
use crate::schema::SchemaProvider;

/// GraphQL object which represents the fields of a document document type as described by it's
/// p2panda schema. A type is added to the root GraphQL schema for every document, as these types
/// are not known at compile time we make use of the `async-graphql ` `dynamic` module.
pub struct DocumentFields;

impl DocumentFields {
    /// Build the fields of a document from the related p2panda schema. Constructs an object which
    /// can then be added to the root GraphQL schema.
    pub fn build(schema: &Schema) -> Object {
        // Construct the document fields object which will be named `<schema_id>Field`.
        let schema_field_name = fields_name(schema.id());
        let mut document_schema_fields = Object::new(&schema_field_name);

        // For every field in the schema we create a type with a resolver.
        for (name, field_type) in schema.fields().iter() {
            // If this is a relation list type we add an argument for filtering items in the list.
            let field = match field_type {
                p2panda_rs::schema::FieldType::RelationList(schema_id)
                | p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
                    Field::new(name, graphql_type(field_type), move |ctx| {
                        FieldFuture::new(async move { Self::resolve(ctx).await })
                    })
                    .argument(
                        InputValue::new("filter", TypeRef::named(filter_name(schema_id)))
                            .description("Filter the query based on field values"),
                    )
                    .argument(
                        InputValue::new("meta", TypeRef::named("MetaFilterInput"))
                            .description("Filter the query based on meta field values"),
                    )
                    .argument(InputValue::new(
                        "orderBy",
                        TypeRef::named(order_by_name(schema.id())),
                    ))
                    .argument(InputValue::new(
                        "orderDirection",
                        TypeRef::named("OrderDirection"),
                    ))
                    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
                    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
                }
                _ => Field::new(name, graphql_type(field_type), move |ctx| {
                    FieldFuture::new(async move { Self::resolve(ctx).await })
                }),
            };
            document_schema_fields = document_schema_fields.field(field);
        }

        document_schema_fields
    }

    /// Resolve a document field value as a graphql `FieldValue`. If the value is a relation, then
    /// the relevant document id or document view id is determined and passed along the query chain.
    /// If the value is a simple type (meaning it is also a query leaf) then it is directly resolved.
    ///
    /// Requires a `ResolverContext` to be passed into the method.
    async fn resolve(ctx: ResolverContext<'_>) -> Result<Option<FieldValue<'_>>, Error> {
        let store = ctx.data_unchecked::<SqlStore>();
        let schema_provider = ctx.data_unchecked::<SchemaProvider>();

        let name = ctx.field().name();

        // Parse the bubble up value.
        let document = downcast_document(&ctx);

        let document = match document {
            DocumentValue::Single(document) => document,
            DocumentValue::Paginated(_, _, document) => document,
        };

        let schema = schema_provider
            .get(document.schema_id())
            .await
            .expect("Schema should be in store");

        // Get the field this query is concerned with.
        match document.get(name).unwrap() {
            // Relation fields are expected to resolve to the related document so we pass
            // along the document id which will be processed through it's own resolver.
            OperationValue::Relation(rel) => {
                // Get the whole document from the store.
                let document = match store.get_document(rel.document_id()).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

                let document = DocumentValue::Single(document);
                Ok(Some(FieldValue::owned_any(document)))
            }
            // Relation lists are handled by collecting and returning a list of all document
            // id's in the relation list. Each of these in turn are processed and queries
            // forwarded up the tree via their own respective resolvers.
            OperationValue::RelationList(rel) => {
                // Get the schema of documents in this relation list.
                let relation_field_schema = schema
                    .fields()
                    .get(name)
                    .expect("Document field should exist on schema");

                // Get the schema itself
                let schema = match relation_field_schema {
                    FieldType::RelationList(schema_id) => {
                        // We can unwrap here as the schema should exist in the store already.
                        schema_provider.get(schema_id).await.unwrap()
                    }
                    _ => panic!(), // Should never reach here.
                };

                // Default pagination, filtering and ordering values.
                let mut pagination = Pagination::<CursorScalar>::default();
                let mut order = Order::default();
                let mut filter = Filter::new();

                // Add all items in the list to the meta_filter `in` filter.
                let list: Vec<OperationValue> =
                    rel.iter().map(|item| item.to_owned().into()).collect();
                filter.add_in(&FilterField::Meta(MetaField::DocumentId), &list);

                // Parse arguments.
                parse_collection_arguments(
                    &ctx,
                    &schema,
                    &mut pagination,
                    &mut order,
                    &mut filter,
                )?;

                // TODO: This needs be be replaced with a query to the db which retrieves a
                // paginated, ordered, filtered collection.
                let mut fields = vec![];
                for document_id in rel.iter() {
                    // Get the whole document from the store.
                    let document = match store.get_document(document_id).await? {
                        Some(document) => document,
                        None => continue,
                    };

                    let document = DocumentValue::Paginated(
                        "CURSOR".to_string(),
                        PaginationData::default(),
                        document,
                    );

                    fields.push(FieldValue::owned_any(document));
                }
                Ok(Some(FieldValue::list(fields)))
            }
            // Pinned relation behaves the same as relation but passes along a document view id.
            OperationValue::PinnedRelation(rel) => {
                // Get the whole document from the store.
                let document = match store.get_document_by_view_id(rel.view_id()).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

                let document = DocumentValue::Single(document);
                Ok(Some(FieldValue::owned_any(document)))
            }
            // Pinned relation lists behave the same as relation lists but pass along view ids.
            OperationValue::PinnedRelationList(rel) => {
                // Get the schema of documents in this relation list.
                let relation_field_schema = schema
                    .fields()
                    .get(name)
                    .expect("Document field should exist on schema");

                // Get the schema itself
                let schema = match relation_field_schema {
                    FieldType::PinnedRelationList(schema_id) => {
                        // We can unwrap here as the schema should exist in the store already.
                        schema_provider.get(schema_id).await.unwrap()
                    }
                    _ => panic!(), // Should never reach here.
                };

                // Default pagination, filtering and ordering values.
                let mut pagination = Pagination::<CursorScalar>::default();
                let mut order = Order::default();
                let mut filter = Filter::new();

                // Add all items in the list to the filter.
                let list: Vec<OperationValue> =
                    rel.iter().map(|item| item.to_owned().into()).collect();
                filter.add_in(&FilterField::Meta(MetaField::DocumentId), &list);

                // Parse arguments.
                parse_collection_arguments(
                    &ctx,
                    &schema,
                    &mut pagination,
                    &mut order,
                    &mut filter,
                )?;

                // TODO: This needs be be replaced with a query to the db which retrieves a
                // paginated, ordered, filtered collection.
                let mut fields = vec![];
                for document_view_id in rel.iter() {
                    // Get the whole document from the store.
                    let document = match store.get_document_by_view_id(document_view_id).await? {
                        Some(document) => document,
                        None => continue,
                    };

                    let document = DocumentValue::Paginated(
                        "CURSOR".to_string(),
                        PaginationData::default(),
                        document,
                    );

                    fields.push(FieldValue::owned_any(document));
                }
                Ok(Some(FieldValue::list(fields)))
            }
            // All other fields are simply resolved to their scalar value.
            value => Ok(Some(FieldValue::value(gql_scalar(value)))),
        }
    }
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response, Value};
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{graphql_test_client, test_runner, TestNode};

    #[rstest]
    // TODO: We don't actually perform any queries yet, these tests will need to be updated
    // when we do.
    #[case(
        r#"(
            first: 10, 
            after: "1_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331", 
            orderBy: OWNER, 
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
                        document {{ 
                            cursor
                            fields {{
                                fields{query_args} {{
                                    document {{
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
}
