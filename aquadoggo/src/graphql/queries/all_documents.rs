// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, ResolverContext, TypeRef};
use async_graphql::indexmap::IndexMap;
use async_graphql::{Error, Value, Name};
use dynamic_graphql::FieldValue;
use log::debug;
use p2panda_rs::schema::Schema;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::types::{DocumentValue, PaginationData};
use crate::graphql::utils::{filter_name, order_by_name, paginated_response_name};

/// Adds GraphQL query for getting all documents of a certain p2panda schema to the root query
/// object.
///
/// The query follows the format `all_<SCHEMA_ID>`.
pub fn build_all_documents_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    query.field(
        Field::new(
            format!("{}{}", constants::QUERY_ALL_PREFIX, schema_id),
            TypeRef::named_list(paginated_response_name(&schema_id)),
            move |ctx| {
                // Take ownership of the schema id in the resolver.
                let schema_id = schema_id.clone();

                debug!(
                    "Query to {}{} received",
                    constants::QUERY_ALL_PREFIX,
                    schema_id
                );

                FieldFuture::new(async move {
                    // Parse all arguments.
                    //
                    // TODO: This is where we will build the abstract filter and query.
                    let (_from, _first, _order_by, _order_direction, _meta, _filter) = parse_arguments(&ctx)?;

                    // Fetch all queried documents and compose the field value list
                    // which will bubble up the query tree.
                    let store = ctx.data_unchecked::<SqlStore>();
                    let documents: Vec<FieldValue> = store
                        .get_documents_by_schema(&schema_id)
                        .await?
                        .iter()
                        .map(|document| {
                            FieldValue::owned_any(DocumentValue::Paginated(
                                "CURSOR".to_string(),
                                PaginationData::default(),
                                document.to_owned(),
                            ))
                        })
                        .collect();

                    // Pass the list up to the children query fields.
                    Ok(Some(FieldValue::list(documents)))
                })
            },
        )
        .argument(
            InputValue::new("filter", TypeRef::named(filter_name(schema.id())))
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
        .description(format!("Get all {} documents.", schema.name())),
    )
}

fn parse_arguments(
    ctx: &ResolverContext,
) -> Result<
    (
        Option<String>,
        Option<i64>,
        Option<Name>,
        Option<Name>,
        Option<IndexMap<Name, Value>>,
        Option<IndexMap<Name, Value>>,
    ),
    Error,
> {
    let mut from = None;
    let mut first = None;
    let mut order_by = None;
    let mut order_direction = None;
    let mut meta = None;
    let mut filter = None;
    for (name, value) in ctx.field().arguments()?.into_iter() {
        match name.as_str() {
            constants::PAGINATION_CURSOR_ARG => from = Some(value.to_string()),
            constants::PAGINATION_FIRST_ARG => {
                if let Value::Number(number) = value {
                    // Argument types are already validated in the graphql api so we can assume this
                    // value to be an integer if present.
                    first = number.as_i64()
                }
            }
            constants::ORDER_BY_ARG => {
                if let Value::Enum(enum_item) = value {
                    order_by = Some(enum_item)
                }
            }
            constants::ORDER_DIRECTION_ARG => {
                if let Value::Enum(enum_item) = value {
                    order_direction = Some(enum_item)
                }
            }
            constants::META_FILTER_ARG => match value {
                Value::Object(index_map) => meta = Some(index_map),
                _ => (),
            },
            constants::FILTER_ARG => {
                if let Value::Object(index_map) = value {
                    filter = Some(index_map)
                }
            }
            _ => (),
        }
    }
    Ok((from, first, order_by, order_direction, meta, filter))
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{add_document, add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
    fn collection_query(#[from(random_key_pair)] key_pair: KeyPair) {
        // Test collection query parameter variations.
        test_runner(move |mut node: TestNode| async move {
            // Add schema to node.
            let schema = add_schema(
                &mut node,
                "schema_name",
                vec![("bool", FieldType::Boolean)],
                &key_pair,
            )
            .await;

            // Publish document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", true.into())],
                &key_pair,
            )
            .await;

            // Configure and send test query.
            let client = graphql_test_client(&node).await;
            let query = format!(
                r#"{{
                collection: all_{type_name} {{
                    hasNextPage
                    totalCount
                    document {{ 
                        fields {{ bool }}
                    }}
                }},
            }}"#,
                type_name = schema.id(),
            );

            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            let response: Response = response.json().await;

            let expected_data = value!({
                "collection": value!([{ "hasNextPage": false, "totalCount": 0, "document": { "fields": { "bool": true, } } }]),
            });
            assert_eq!(response.data, expected_data, "{:#?}", response.errors);
        });
    }
}
