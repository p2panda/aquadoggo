// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ObjectAccessor, TypeRef, ValueAccessor,
};
use async_graphql::indexmap::IndexMap;
use async_graphql::Error;
use dynamic_graphql::Value;
use log::{debug, info};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema};
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::query::{Direction, Field as FilterField, Filter, MetaField, Order};
use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::types::{DocumentValue, PaginationData};
use crate::graphql::utils::{
    filter_name, filter_to_operation_value, order_by_name, paginated_response_name,
};
use crate::schema::SchemaProvider;

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
                    let schema_provider = ctx.data_unchecked::<SchemaProvider>();

                    // Default pagination, filtering and ordering values.
                    let mut from = None;
                    let mut first = None;
                    let mut order_by = FilterField::Meta(MetaField::DocumentId);
                    let mut order_direction = Direction::Ascending;
                    let mut meta = Filter::new();
                    let mut filter = Filter::new();

                    // Get the schema for the document type being queried.
                    let schema = schema_provider
                        .get(&schema_id)
                        .await
                        .expect("Schema should exist in schema provider");

                    // Parse all argument values based on expected keys and types.
                    for (name, value) in ctx.args.iter() {
                        match name.as_str() {
                            constants::PAGINATION_CURSOR_ARG => from = Some(value.string()?),
                            constants::PAGINATION_FIRST_ARG => first = Some(value.i64()?),
                            constants::ORDER_BY_ARG => {
                                order_direction = match value.enum_name()? {
                                    "asc" => Direction::Ascending,
                                    "desc" => Direction::Descending,
                                    _ => panic!("Unknown order by argument key received"),
                                };
                            }
                            constants::ORDER_DIRECTION_ARG => {
                                order_by = match value.enum_name()? {
                                    "OWNER" => FilterField::Meta(MetaField::Owner),
                                    field_name => FilterField::new(field_name),
                                };
                            }
                            constants::META_FILTER_ARG => {
                                let filter_object = value
                                    .object()
                                    .map_err(|_| Error::new("internal: is not an object"))?;
                                parse_meta_filter(&mut meta, &filter_object)?;
                            }
                            constants::FILTER_ARG => {
                                let filter_object = value
                                    .object()
                                    .map_err(|_| Error::new("internal: is not an object"))?;
                                parse_filter(&mut filter, &schema, &filter_object)?;
                            }
                            _ => panic!("Unknown argument key received"),
                        }
                    }

                    // Construct the order struct.
                    let order = Order::new(&order_by, &order_direction);

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

/// Parse a filter object received from the graphql api into an abstract filter type based on the
/// schema of the documents being queried.
fn parse_filter(
    filter: &mut Filter,
    schema: &Schema,
    filter_object: &ObjectAccessor,
) -> Result<(), Error> {
    for (field, filters) in filter_object.iter() {
        let filter_field = FilterField::new(field.as_str());
        let filters = filters.object()?;
        for (name, value) in filters.iter() {
            let field_type = schema.fields().get(field.as_str()).unwrap();
            match name.as_str() {
                "in" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, field_type)?;
                        list_items.push(item);
                    }
                    filter.add_in(&filter_field, &list_items);
                }
                "notIn" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, field_type)?;
                        list_items.push(item);
                    }
                    filter.add_not_in(&filter_field, &list_items);
                }
                "eq" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add(&filter_field, &value);
                }
                "notEq" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_not(&filter_field, &value);
                }
                "gt" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_gt(&filter_field, &value);
                }
                "gte" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_gte(&filter_field, &value);
                }
                "lt" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_lt(&filter_field, &value);
                }
                "lte" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_lte(&filter_field, &value);
                }
                "contains" => {
                    filter.add_contains(&filter_field, &value.string()?);
                }
                "notContains" => {
                    filter.add_contains(&filter_field, &value.string()?);
                }
                _ => panic!("Unknown filter type received"),
            }
        }
    }
    Ok(())
}

/// Parse a meta filter object received from the graphql api into an abstract filter type based on the
/// schema of the documents being queried.
fn parse_meta_filter(filter: &mut Filter, filter_object: &ObjectAccessor) -> Result<(), Error> {
    for (field, filters) in filter_object.iter() {
        let meta_field = MetaField::try_from(field.as_str())?;
        let filter_field = FilterField::Meta(meta_field);
        let filters = filters.object()?;
        for (name, value) in filters.iter() {
            match name.as_str() {
                "in" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &FieldType::String)?;
                        list_items.push(item);
                    }
                    filter.add_in(&filter_field, &list_items);
                }
                "notIn" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &FieldType::String)?;
                        list_items.push(item);
                    }
                    filter.add_not_in(&filter_field, &list_items);
                }
                "eq" => {
                    let value = filter_to_operation_value(&value, &FieldType::String)?;
                    filter.add(&filter_field, &value);
                }
                "notEq" => {
                    let value = filter_to_operation_value(&value, &FieldType::String)?;
                    filter.add_not(&filter_field, &value);
                }
                _ => panic!("Unknown meta filter type received"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use async_graphql::{value, Response, Value};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;
    use serde_json::json;

    use crate::test_utils::{add_document, add_schema, graphql_test_client, test_runner, TestNode};

    #[rstest]
    // TODO: We don't validate all of the internal argument values yet, only the simple types and
    // object fields, these tests will need updating when we do.
    //
    // TODO: We don't actually perform any validation yet, these tests will need to be updated
    // when we do.
    #[case(
        "".to_string(), 
        value!({
            "collection": value!([{ "hasNextPage": false, "totalCount": 0, "document": { "cursor": "CURSOR", "fields": { "bool": true, } } }]),
        }),
        vec![]
    )]
    #[case(
        r#"(first: 10, after: "CURSOR", orderBy: OWNER, orderDirection: ASC, filter: { bool : { eq: true } }, meta: { owner: { in: ["PUBLIC"] } })"#.to_string(), 
        value!({
            "collection": value!([{ "hasNextPage": false, "totalCount": 0, "document": { "cursor": "CURSOR", "fields": { "bool": true, } } }]),
        }),
        vec![]
    )]
    #[case(
        r#"(first: "hello")"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"first\", expected type \"Int\"".to_string()]
    )]
    #[case(
        r#"(after: HELLO)"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"String\"".to_string()]
    )]
    #[case(
        r#"(after: 27)"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"String\"".to_string()]
    )]
    #[case(
        r#"(orderBy: HELLO)"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331OrderBy\" does not contain the value \"HELLO\"".to_string()]
    )]
    #[case(
        r#"(orderBy: "hello")"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"orderBy\", enumeration type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331OrderBy\" does not contain the value \"hello\"".to_string()]
    )]
    #[case(
        r#"(orderDirection: HELLO)"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"orderDirection\", enumeration type \"OrderDirection\" does not contain the value \"HELLO\"".to_string()]
    )]
    #[case(
        r#"(orderDirection: "hello")"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"orderDirection\", enumeration type \"OrderDirection\" does not contain the value \"hello\"".to_string()]
    )]
    #[case(
        r#"(filter: "hello")"#.to_string(), 
        Value::Null,
        vec!["internal: is not an object".to_string()]
    )]
    #[case(
        r#"(filter: { bool: { in: ["hello"] }})"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"filter.bool\", unknown field \"in\" of type \"BooleanFilter\"".to_string()]
    )]
    #[case(
        r#"(filter: { hello: { eq: true }})"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"filter\", unknown field \"hello\" of type \"schema_name_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331Filter\"".to_string()]
    )]
    #[case(
        r#"(filter: { bool: { contains: "hello" }})"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"filter.bool\", unknown field \"contains\" of type \"BooleanFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: "hello")"#.to_string(), 
        Value::Null,
        vec!["internal: is not an object".to_string()]
    )]
    #[case(
        r#"(meta: { bool: { in: ["hello"] }})"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"meta\", unknown field \"bool\" of type \"MetaFilterInput\"".to_string()]
    )]
    #[case(
        r#"(meta: { owner: { contains: "hello" }})"#.to_string(), 
        Value::Null,
        vec!["Invalid value for argument \"meta.owner\", unknown field \"contains\" of type \"OwnerFilter\"".to_string()]
    )]

    fn collection_query(
        key_pair: KeyPair,
        #[case] query_args: String,
        #[case] expected_data: Value,
        #[case] expected_errors: Vec<String>,
    ) {
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
                collection: all_{type_name}{query_args} {{
                    hasNextPage
                    totalCount
                    document {{ 
                        cursor
                        fields {{ bool }}
                    }}
                }},
            }}"#,
                type_name = schema.id(),
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

            // Assert error messages.
            let err_msgs: Vec<String> = response
                .errors
                .iter()
                .map(|err| err.message.to_string())
                .collect();
            assert_eq!(err_msgs, expected_errors);
        });
    }
}
