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
use crate::proptests::utils::{add_documents_from_ast, add_schemas_from_ast};
use crate::test_utils::{graphql_test_client, test_runner, TestClient, TestNode};

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

/// Unwrap values from a GraphQL response.
fn unwrap_response(response: &Response) -> (bool, i64, Option<String>, Vec<JsonValue>) {
    let query_response = response
        .data
        .to_owned()
        .into_json()
        .expect("Can convert to json")
        .as_object()
        .expect("Query response is object")
        .to_owned();

    let pagination_root = query_response.get("query").unwrap();

    let has_next_page = pagination_root.get("hasNextPage").unwrap();
    let total_count = pagination_root.get("totalCount").unwrap();
    let end_cursor = pagination_root.get("endCursor").unwrap();
    let documents = pagination_root
        .get("documents")
        .unwrap()
        .as_array()
        .expect("Documents fields is array");
    (
        has_next_page.as_bool().expect("Is boolean value"),
        total_count.as_i64().expect("Is integer"),
        end_cursor.as_str().map(String::from),
        documents.clone(),
    )
}

/// Perform a paginated query over a collection of documents.
async fn make_query(
    client: &TestClient,
    schema_id: &SchemaId,
    query_args: &str,
) -> (bool, i64, Option<String>, Vec<JsonValue>) {
    // Helper for creating queries
    let query = |type_name: &SchemaId, query_args: &str| -> String {
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
    };

    let response = client
        .post("/graphql")
        .json(&json!({ "query": query(&schema_id, query_args) }))
        .send()
        .await;

    let response: Response = response.json().await;
    assert!(response.is_ok());
    unwrap_response(&response)
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

/// Perform tests for querying paginated collections of documents identified by their schema.
async fn paginated_query_meta_fields_only(
    client: &TestClient,
    schema_id: &SchemaId,
    expected_documents: &Vec<DocumentViewId>,
) {
    let mut query_args = "(first: 5)".to_string();
    let total_documents = expected_documents.len() as i64;
    let mut remaining_documents = expected_documents.len();

    loop {
        // Make a paginated query.
        let (has_next_page, total_count, end_cursor, documents) =
            make_query(&client, &schema_id, &query_args).await;

        // Total count should match number of documents expected.
        assert_eq!(total_count, total_documents);

        // If the returned number of documents == 0....
        if documents.is_empty() {
            // There should be no next page.
            assert!(!has_next_page);
            // The end cursor should be None.
            assert!(end_cursor.is_none());
            // End the test here.
            break;
        }

        // In all other cases there should be....

        // A next cursor given which matches the cursor of the last returned document.
        let last_document_cursor = documents
            .last()
            .unwrap()
            .as_object()
            .unwrap()
            .get("cursor")
            .unwrap();
        assert_eq!(
            end_cursor.as_ref().unwrap(),
            last_document_cursor.as_str().unwrap()
        );

        // If there are more than 5 remaining documents then we expect...
        if remaining_documents > 5 {
            // The number of returned documents to match the pagination rate.
            assert_eq!(documents.len(), 5);
            // There should be a next page available.
            assert!(has_next_page);
        }

        // If there are fewer or equal than 5 documents remaingin...
        if remaining_documents <= 5 {
            // There should be no next page available.
            assert!(!has_next_page);
            // The number of documents returned should match the remaining documents.
            assert_eq!(documents.len(), remaining_documents);
        }

        // De-increment the remaining documents count.
        remaining_documents -= documents.len();
        // Compose the next query arguments.
        query_args = format!("(first: 5, after: \"{0}\")", end_cursor.unwrap());
    }
}

proptest! {
    #![proptest_config(Config {
        cases: 100,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        .. Config::default()
      })]
    #[test]
    fn test_query((schema_ast, document_ast_collection) in schema_with_documents_strategy()) {
        // The proptest strategies for generating schema and deriving collections of documents for each injects
        // the raw AST types into the test. Here we convert these into p2panda `Entries`, `Operations` and `Schema`
        // which we can then use to populate a store and run queries against.

        // Now we start up a test runner and inject a test node we can populate.
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
                paginated_query_meta_fields_only(&client, &schema_id, &schema_documents).await;
            };
        });
    }
}
