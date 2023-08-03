// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::Response;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use proptest::test_runner::{Config, FileFailurePersistence};
use proptest::{prop_compose, proptest, strategy::Just};
use serde_json::{json, Value as JsonValue};

use crate::proptests::document_strategies::{documents_strategy, DocumentAST};
use crate::proptests::schema_strategies::{schema_strategy, SchemaAST};
use crate::proptests::utils::{
    add_documents_from_ast, add_schemas_from_ast, parse_filter, parse_selected_fields, FieldName,
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
    assert_eq!(schemas.len(), node_schemas.len() - 4); // minus 4 for system schema
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

        // Reduce the remaining documents count by the number of documents returned from the query.
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
            -> (SchemaAST, Vec<DocumentAST>, Vec<((FieldName, Filter), Vec<(FieldName, Filter)>)>, Vec<(MetaField, Filter)>) {
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
    /// Test pagination for collection queries. Schemas and documents are generated via proptest
    /// strategies. Tests expected behavior and return values based on the number of documents
    /// which were generated for each schema. Performs a query where only meta fields are selected
    /// and one where only document fields are selected.
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
                let fields = parse_selected_fields(&node, &schema, None).await;
                paginated_query(&client, &schema_id, &schema_documents, Some(&fields), paginated_query_application_fields).await;
            };
        });
    }

    #[test]
    /// Test passing different combinations of filter arguments to the root collection query and
    /// also to fields which contain relation lists. Filter arguments are generated randomly via
    /// proptest strategies and parsed into valid graphql query strings.
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

            // Get the root schema from the provider.
            let schema = node.context.schema_provider.get(&schema_ast.id).await.expect("Schema should exist on node");

            // Filter args for the root collection query
            let mut root_filter_args = Vec::new();

            // Filter args for list fields
            let mut list_filter_args = HashMap::new();

            // Parse filter arg strings for the root collection query and list fields
            // sub-collection queries.
            for ((name, root_filter), list_filters) in &application_field_filters {
                // Parse the root filter args.
                parse_filter(&mut root_filter_args, name, root_filter);

                // If list field filters were generated parse them here.
                if !list_filters.is_empty() {
                    let mut args = Vec::new();
                    for (list_field_name, list_filter) in list_filters {
                        parse_filter(&mut args, list_field_name, list_filter);
                    }
                    list_filter_args.insert(name, args);
                }
            }

            // Join the root query args into one string.
            let filter_args_str = root_filter_args.join(", ");
            let filter_args_str = format!("( filter: {{ {filter_args_str} }} )");

            // Construct valid GraphQL query for all fields for the schema and relations. Any
            // relation list filter arguments are added already here.
            let fields = parse_selected_fields(&node, &schema, Some(&list_filter_args)).await;

            // Make the query.
            let _data = make_query(
                &client,
                &paginated_query_application_fields(&schema.id(), &filter_args_str, Some(&fields)),
            )
            .await;
        });
    }
}
