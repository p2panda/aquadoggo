// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use log::debug;
use p2panda_rs::schema::Schema;

use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::types::DocumentValue;
use crate::graphql::utils::{
    paginated_response_name, parse_collection_arguments, with_collection_arguments,
};
use crate::schema::SchemaProvider;

/// Adds a GraphQL query for retrieving a paginated, ordered and filtered collection of documents
/// by schema to the passed root query object.
///
/// The query follows the format `all_<SCHEMA_ID>(<...ARGS>)`.
pub fn build_all_documents_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    query
        .field(with_collection_arguments(
            Field::new(
                format!("{}{}", constants::QUERY_ALL_PREFIX, schema_id),
                TypeRef::named_list(paginated_response_name(&schema_id)),
                move |ctx| {
                    // Take ownership of the schema id in the resolver
                    let schema_id = schema_id.clone();

                    debug!(
                        "Query to {}{} received",
                        constants::QUERY_ALL_PREFIX,
                        schema_id
                    );

                    FieldFuture::new(async move {
                        let schema_provider = ctx.data_unchecked::<SchemaProvider>();
                        let store = ctx.data_unchecked::<SqlStore>();

                        // Get the schema for the document type being queried
                        let schema = schema_provider
                            .get(&schema_id)
                            .await
                            .expect("Schema should exist in schema provider");

                        // Populate query arguments with values from GraphQL query
                        let query = parse_collection_arguments(&ctx, &schema)?;

                        // Fetch all queried documents and compose the field value list which will
                        // bubble up the query tree
                        let (pagination_data, documents) =
                            store.query(&schema, &query, None).await?;

                        let results: Vec<FieldValue> = documents
                            .into_iter()
                            .map(|(cursor, document)| {
                                FieldValue::owned_any(DocumentValue::Paginated(
                                    cursor,
                                    pagination_data.clone(),
                                    document,
                                ))
                            })
                            .collect();

                        // Pass the list up to the children query fields
                        Ok(Some(FieldValue::list(results)))
                    })
                },
            ),
            schema.id(),
        ))
        .description(format!(
            "Query a paginated collection of `{}` documents. \
               The requested collection is filtered and ordered following \
               parameters passed into the query via the available arguments.",
            schema.id().name()
        ))
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
    // TODO: We don't actually perform any queries yet, these tests will need to be updated
    // when we do.
    #[case(
        "".to_string(),
        value!({
            "collection": value!([{ "hasNextPage": false, "totalCount": 0, "document": { "cursor": "CURSOR", "fields": { "bool": true, } } }]),
        }),
        vec![]
    )]
    #[case(
        r#"(
            first: 10,
            after: "1_00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331",
            orderBy: OWNER,
            orderDirection: ASC,
            filter: {
                bool : {
                    eq: true
                }
            },
            meta: {
                owner: {
                    in: ["7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982"]
                },
                documentId: {
                    eq: "00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331"
                },
                viewId: {
                    notIn: ["00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331"]
                }
            }
        )"#.to_string(),
        value!({
            "collection": value!([{ "hasNextPage": false, "totalCount": 0, "document": { "cursor": "CURSOR", "fields": { "bool": true, } } }]),
        }),
        vec![]
    )]
    #[case(
        r#"(first: 0)"#.to_string(),
        Value::Null,
        vec!["out of range integral type conversion attempted".to_string()]
    )]
    #[case(
        r#"(first: "hello")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"first\", expected type \"Int\"".to_string()]
    )]
    #[case(
        r#"(after: HELLO)"#.to_string(),
        Value::Null,
        vec!["internal: not a string".to_string()]
    )]
    #[case(
        r#"(after: "00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331")"#.to_string(),
        Value::Null,
        vec!["Invalid amount of cursor parts".to_string()]
    )]
    #[case(
        r#"(after: 27)"#.to_string(),
        Value::Null,
        vec!["internal: not a string".to_string()]
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
    #[case(
        r#"(meta: { documentId: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.documentId\", unknown field \"contains\" of type \"DocumentIdFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: { viewId: { contains: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.viewId\", unknown field \"contains\" of type \"DocumentViewIdFilter\"".to_string()]
    )]
    #[case(
        r#"(meta: { documentId: { eq: 27 }})"#.to_string(),
        Value::Null,
        vec!["internal: not a string".to_string()]
    )]
    #[case(
        r#"(meta: { viewId: { in: "hello" }})"#.to_string(),
        Value::Null,
        vec!["internal: not a list".to_string()]
    )]
    // TODO: When we have a way to add custom validation to scalar types then this case should
    // fail as we pass in an invalid public key string. Same for documentId and viewId meta fields.
    // #[case(
    //     r#"(meta: { owner: { eq: "hello" }})"#.to_string(),
    //     Value::Null,
    //     vec!["Invalid value for argument \"meta.owner\", unknown field \"contains\" of type \"OwnerFilter\"".to_string()]
    // )]

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
