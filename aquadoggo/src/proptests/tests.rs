// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::Response;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use p2panda_rs::test_utils::fixtures::random_document_view_id;
use proptest::test_runner::{Config, FileFailurePersistence};
use proptest::{prop_compose, proptest, strategy::Just};
use serde_json::{json, Value as JsonValue};

use crate::proptests::document_strategies::{documents_strategy, DocumentAST};
use crate::proptests::filter_strategies::FilterValue;
use crate::proptests::schema_strategies::{schema_strategy, SchemaAST};
use crate::proptests::utils::{
    add_documents_from_ast, add_schemas_from_ast, parse_selected_fields, FieldName,
};
use crate::test_utils::{graphql_test_client, test_runner, TestClient, TestNode};

use super::filter_strategies::{
    application_filters_strategy, meta_field_filter_strategy, Filter, MetaField,
};

/// Check the node is in the expected state.
async fn sanity_checks(
    node: &TestNode,
    documents_by_schema: &HashMap<SchemaId, Vec<DocumentViewId>>,
    schemas: &Vec<SchemaId>,
) {
    let node_schemas = node.context.schema_provider.all().await;
    assert_eq!(schemas.len(), node_schemas.len() - 2); // minus 2 for system schema
    for schema_id in schemas {
        let result = node
            .context
            .store
            .get_documents_by_schema(schema_id)
            .await
            .expect("Documents of this schema expected on node");
        assert_eq!(
            result.len(),
            documents_by_schema
                .get(&schema_id)
                .cloned()
                .unwrap_or_default()
                .len()
        );
    }
}

/// Make a GraphQL query to the node.
async fn make_query(client: &TestClient, query_str: &str) -> JsonValue {
    let response = client
        .post("/graphql")
        .json(&json!({ "query": query_str }))
        .send()
        .await;

    let response: Response = response.json().await;
    assert!(response.is_ok(), "{:?}", response.errors);
    response.data.into_json().expect("Can convert to json")
}

// Helper for creating paginated queries over meta fields.
fn paginated_query_meta_fields_only(
    type_name: &SchemaId,
    query_args: &str,
    _application_fields: Option<&Vec<String>>,
) -> String {
    format!(
        r#"{{
            query: all_{type_name}{query_args} {{
                hasNextPage
                totalCount
                endCursor
                documents {{
                    cursor
                    meta {{
                        owner
                        documentId
                        viewId
                    }}
                }}
            }},
        }}"#
    )
}

// Helper for creating paginated queries over application fields.
fn paginated_query_application_fields(
    type_name: &SchemaId,
    query_args: &str,
    application_fields: Option<&Vec<String>>,
) -> String {
    let application_fields = application_fields.expect("Application fields must be passed");
    let fields_str = application_fields.join(" ");
    let query = format!(
        r#"{{
            query: all_{type_name}{query_args} {{
                hasNextPage
                totalCount
                endCursor
                documents {{
                    cursor
                    fields {{
                        {fields_str}
                    }}
                }}
            }},
        }}"#
    );
    println!("{query}");
    query
}

/// Perform tests for querying paginated collections of documents identified by their schema.
async fn paginated_query(
    client: &TestClient,
    schema_id: &SchemaId,
    expected_documents: &Vec<DocumentViewId>,
    application_fields: Option<&Vec<String>>,
    query_builder: impl Fn(&SchemaId, &str, Option<&Vec<String>>) -> String,
) {
    let mut query_args = "(first: 5)".to_string();
    let mut expected_remaining_documents = expected_documents.len();

    loop {
        // Make a paginated query.
        let data = make_query(
            &client,
            &query_builder(&schema_id, &query_args, application_fields),
        )
        .await;

        let documents = data["query"]["documents"].as_array().unwrap();
        let total_count = data["query"]["totalCount"].clone();
        let has_next_page = data["query"]["hasNextPage"].clone();
        let end_cursor = data["query"]["endCursor"].clone();

        // Total count should match number of documents expected.
        assert_eq!(total_count, json!(expected_documents.len()));

        if expected_remaining_documents == 0 {
            assert_eq!(has_next_page, json!(false));
            assert_eq!(end_cursor, JsonValue::Null);
            assert_eq!(documents.len(), 0);
            // Pagination is complete and the test is over.
            break;
        }

        // A next cursor given which matches the cursor of the last returned document.
        let last_document_cursor = documents.last().unwrap()["cursor"].clone();
        assert_eq!(end_cursor, last_document_cursor);

        if expected_remaining_documents > 5 {
            assert_eq!(documents.len(), 5);
            assert_eq!(has_next_page, json!(true));
        } else {
            assert_eq!(has_next_page, json!(false));
            assert_eq!(documents.len(), expected_remaining_documents);
        }

        // De-increment the remaining documents count.
        expected_remaining_documents -= documents.len();
        // Compose the next query arguments.
        query_args = format!("(first: 5, after: {})", end_cursor);
    }
}

prop_compose! {
    /// Strategy for generating schemas, and documents from these schema.
    fn schema_with_documents_strategy()
            (schema in schema_strategy())
            (documents in documents_strategy(schema.clone()), schema in Just(schema))
            -> (SchemaAST, Vec<DocumentAST>) {
        (schema, documents)
    }
}

prop_compose! {
    /// Strategy for generating schemas, and documents from these schema.
    fn schema_with_documents_and_filters_strategy()
            (schema in schema_strategy())
            (documents in documents_strategy(schema.clone()), schema in Just(schema.clone()), application_filters in application_filters_strategy(schema.clone().fields), meta_filters in meta_field_filter_strategy())
            -> (SchemaAST, Vec<DocumentAST>, Vec<(Filter, FieldName)>, Vec<(MetaField, Filter)>) {
        (schema, documents, application_filters, meta_filters)
    }
}

proptest! {
    #![proptest_config(Config {
        cases: 100,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        .. Config::default()
    })]
    #[test]
    fn pagination_queries((schema_ast, document_ast_collection) in schema_with_documents_strategy()) {
        test_runner(|mut node: TestNode| async move {
            // Add all schema to the node.
            let mut schemas = Vec::new();
            add_schemas_from_ast(&mut node, &schema_ast, &mut schemas).await;

            // Add all documents to the node.
            let mut documents = HashMap::new();
            for document_ast in document_ast_collection.iter() {
                add_documents_from_ast(&mut node, &document_ast, &mut documents).await;
            }

            // Perform a couple of sanity checks to make sure the test node is in the state we expect.
            sanity_checks(&node, &documents, &schemas).await;

            // Create a GraphQL client.
            let client = graphql_test_client(&node).await;

            // Run the test for each schema and related documents that have been generated.
            for schema_id in schemas {
                let schema_documents = documents.get(&schema_id).cloned().unwrap_or_default();

                // Perform tests for paginated collection queries with only meta fields selected.
                paginated_query(&client, &schema_id, &schema_documents, None, paginated_query_meta_fields_only).await;

                let schema = node.context.schema_provider.get(&schema_id).await.expect("Schema should exist on node");
                let fields_vec = parse_selected_fields(&node, &schema).await;
                println!("{schema_documents:#?}");
                paginated_query(&client, &schema_id, &schema_documents, Some(&fields_vec), paginated_query_application_fields).await;
            };
        });
    }

    #[test]
    fn filtering_queries((schema_ast, document_ast_collection, application_field_filters, _meta_field_filters) in schema_with_documents_and_filters_strategy()) {

        test_runner(|mut node: TestNode| async move {
            // Add all schema to the node.
            let mut schemas = Vec::new();
            add_schemas_from_ast(&mut node, &schema_ast, &mut schemas).await;

            // Add all documents to the node.
            let mut documents = HashMap::new();
            for document_ast in document_ast_collection.iter() {
                add_documents_from_ast(&mut node, &document_ast, &mut documents).await;
            }

            // Perform a couple of sanity checks to make sure the test node is in the state we expect.
            sanity_checks(&node, &documents, &schemas).await;

            // Create a GraphQL client.
            let client = graphql_test_client(&node).await;

            // Run the test for each schema and related documents that have been generated.
            let schema_documents = documents.get(&schema_ast.id).cloned().unwrap_or_default();
            let schema = node.context.schema_provider.get(&schema_ast.id).await.expect("Schema should exist on node");

            let mut filter_args = Vec::new();
            for (filter, name) in &application_field_filters {
                let name = name.clone().0;
                let document_id_or_view_id = schema_documents.clone().into_iter().next().unwrap_or(random_document_view_id()).to_string();
                match filter {
                    Filter::Contains(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ contains: \"\"\"{value}\"\"\" }}")),
                            _ => panic!(),
                        }
                    },
                    Filter::NotContains(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ notContains: \"\"\"{value}\"\"\" }}")),
                            _ => panic!(),
                        }
                    },
                    Filter::Equal(value) => {
                        match value {
                            FilterValue::UniqueIdentifier => filter_args.push(format!("{name}: {{ eq: \"{document_id_or_view_id}\" }}")),
                            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ eq: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
                        }
                    },
                    Filter::NotEqual(value) => {
                        match value {
                            FilterValue::UniqueIdentifier => filter_args.push(format!("{name}: {{ notEq: \"{document_id_or_view_id}\" }}")),
                            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ notEq: {value} }}")),
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ notEq: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ notEq: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ notEq: {value} }}")),
                        }
                    },
                    Filter::IsIn(value) => {
                        match value {
                            FilterValue::UniqueIdentifier => filter_args.push(format!("{name}: {{ in: [\"{document_id_or_view_id}\"] }}")),
                            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ in: [\"\"\"{value}\"\"\"] }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
                        }
                    },
                    Filter::NotIn(value) => {
                        match value {
                            FilterValue::UniqueIdentifier => filter_args.push(format!("{name}: {{ notIn: [\"{document_id_or_view_id}\"] }}")),
                            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ notIn: [{value}] }}")),
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ notIn: [\"\"\"{value}\"\"\"] }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ notIn: [{value}] }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ notIn: [{value}] }}")),
                        }
                    },
                    Filter::GreaterThan(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ gt: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ gt: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ gt: {value} }}")),
                            _ => panic!(),

                        }
                    },
                    Filter::LessThan(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ lt: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ lt: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ lt: {value} }}")),
                            _ => panic!(),
                        }
                    },
                    Filter::GreaterThanOrEqual(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ gte: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ gte: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ gte: {value} }}")),
                            _ => panic!(),

                        }
                    },
                    Filter::LessThanOrEqual(value) => {
                        match value {
                            FilterValue::String(value) => filter_args.push(format!("{name}: {{ lte: \"\"\"{value}\"\"\" }}")),
                            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ lte: {value} }}")),
                            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ lte: {value} }}")),
                            _ => panic!(),
                        }
                    },
                }
            }
            let filter_args_str = filter_args.join(", ");
            let filter_args_str = format!("( first: 1000, filter: {{ {filter_args_str} }} )");
            let fields_vec = parse_selected_fields(&node, &schema).await;
            let _data = make_query(
                &client,
                &paginated_query_application_fields(&schema.id(), &filter_args_str, Some(&fields_vec)),
            )
            .await;
        });
    }
}
