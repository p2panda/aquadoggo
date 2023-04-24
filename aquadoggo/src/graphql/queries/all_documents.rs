// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, TypeRef};
use log::debug;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::types::DocumentCollection;
use crate::graphql::utils::{collection_name, with_collection_arguments};

/// Adds a GraphQL query for retrieving a paginated, ordered and filtered collection of documents
/// by schema to the passed root query object.
///
/// The query follows the format `all_<SCHEMA_ID>(<...ARGS>)`.
pub fn build_all_documents_query(query: Object, schema: &Schema) -> Object {
    let schema_id = schema.id().clone();
    let schema = schema.clone();
    query
        .field(with_collection_arguments(
            Field::new(
                format!("{}{}", constants::QUERY_ALL_PREFIX, schema_id),
                TypeRef::named_nn(collection_name(&schema_id)),
                move |ctx| {
                    let schema = schema.clone();
                    debug!(
                        "Query to {}{} received",
                        constants::QUERY_ALL_PREFIX,
                        schema.id()
                    );

                    FieldFuture::new(
                        async move { DocumentCollection::resolve(ctx, schema, None).await },
                    )
                },
            ),
            &schema_id,
        ))
        .description(format!(
            "Query a paginated collection of `{}` documents. \
               The requested collection is filtered and ordered following \
               parameters passed into the query via the available arguments.",
            schema_id.name()
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
    #[case(
        "".to_string(),
        value!({
            "collection": value!({
                "hasNextPage": false,
                "totalCount": 2,
                "endCursor": "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                "documents": [
                    {
                        "cursor": "273AmFQTk7w6134GhzKUS5tY8qDuaMYBPgbaftZ43G7saiKa73MPapFvjNDixbNjCr5ucNqzNsx2fYdRqRod9U2W",
                        "fields": { "bool": true, },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "00200436216389856afb3f3a7d8cb2d2981be85787aebed02031c72eb9c216406c57",
                            "viewId": "00200436216389856afb3f3a7d8cb2d2981be85787aebed02031c72eb9c216406c57",
                        }
                    },
                    {
                        "cursor": "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                        "fields": { "bool": false, },
                        "meta": {
                            "owner": "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96",
                            "documentId": "0020de552d81948f220d09127dc42963071d086a142c9547e701674d4cac83f29872",
                            "viewId": "0020de552d81948f220d09127dc42963071d086a142c9547e701674d4cac83f29872",
                        }
                    }
                ]
            }),
        }),
        vec![]
    )]
    #[case(
        r#"
            (
                first: 1,
                after: "31Ch6qa4mdKcxpWJG4X9Wf5iMvSSxmSGg8cyg9teNR6yKmLncZCmyVUaPFjRNoWcxpeASGqrRiJGR8HSqjWBz5HE",
                orderBy: DOCUMENT_ID,
                orderDirection: ASC,
                filter: {
                    bool: {
                        eq: false
                    }
                }
            )
        "#.to_string(),
        value!({
            "collection": value!({
                "hasNextPage": false,
                "totalCount": 1,
                "endCursor": Value::Null,
                "documents": []
            }),
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
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
    )]
    #[case(
        r#"(after: "00205406410aefce40c5cbbb04488f50714b7d5657b9f17eed7358da35379bc20331")"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
    )]
    #[case(
        r#"(after: 27)"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"after\", expected type \"Cursor\"".to_string()]
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
        vec!["Invalid value for argument \"meta.documentId.eq\", expected type \"DocumentId\"".to_string()]
    )]
    #[case(
        r#"(meta: { viewId: { in: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.viewId.in\", expected type \"DocumentViewId\"".to_string()]
    )]
    #[case(
        r#"(meta: { owner: { eq: "hello" }})"#.to_string(),
        Value::Null,
        vec!["Invalid value for argument \"meta.owner.eq\", expected type \"PublicKey\"".to_string()]
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

            // Publish another document on node.
            add_document(
                &mut node,
                schema.id(),
                vec![("bool", false.into())],
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
                    endCursor
                    documents {{
                        cursor
                        fields {{
                            bool
                        }}
                        meta {{
                            owner
                            documentId
                            viewId
                        }}
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
