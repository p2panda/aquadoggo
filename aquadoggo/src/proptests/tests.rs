// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::Response;
use p2panda_rs::schema::SchemaId;
use proptest::test_runner::{Config, FileFailurePersistence};
use proptest::{prop_compose, proptest, strategy::Just};
use serde_json::{json, Value as JsonValue};

use crate::proptests::document_strategies::{documents_strategy, DocumentAST};
use crate::proptests::schema_strategies::{schema_strategy, SchemaAST};
use crate::proptests::utils::{add_documents_from_ast, add_schemas_from_ast};
use crate::test_utils::{graphql_test_client, test_runner, TestClient, TestNode};

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
    fn schema_with_documents_strategy()
            (schema in schema_strategy())
            (documents in documents_strategy(schema.clone()), schema in Just(schema))
            -> (SchemaAST, Vec<DocumentAST>) {
        (schema, documents)
    }
}

async fn paginated_query_meta_fields_only(
    client: &TestClient,
    schema_id: &SchemaId,
    expected_total_documents: usize,
) {
    let mut query_args = "".to_string();

    loop {
        let (has_next_page, total_count, end_cursor, documents) =
            make_query(&client, &schema_id, &query_args).await;
        let end_cursor = end_cursor.unwrap(); // We expect every request here to have an end cursor
        query_args = format!("(after: \"{0}\")", end_cursor);
        let last_document_cursor = documents
            .last()
            .unwrap()
            .as_object()
            .unwrap()
            .get("cursor")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(end_cursor, last_document_cursor);
        assert_eq!(total_count, expected_total_documents as i64);
        if has_next_page {
            assert_eq!(documents.len(), 25)
        } else {
            let remaining_documents = match expected_total_documents % 25 {
                0 => 25,
                remaining => remaining,
            };
            assert_eq!(documents.len(), remaining_documents);
            let (has_next_page, total_count, end_cursor, documents) =
                make_query(&client, &schema_id, &query_args).await;
            assert_eq!(end_cursor, None);
            assert_eq!(has_next_page, false);
            assert_eq!(documents.len(), 0);
            assert_eq!(total_count, expected_total_documents as i64);
            break;
        }
    }
}

proptest! {
    #![proptest_config(Config {
        cases: 100,
        max_shrink_time: 60000,
        max_shrink_iters: 1000,
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
            add_schemas_from_ast(&mut node, &schema_ast).await;

            // Add all documents to the node.
            let mut documents = HashMap::new();
            for document_ast in document_ast_collection.iter() {
                add_documents_from_ast(&mut node, &document_ast, &mut documents).await;
            }

            let client = graphql_test_client(&node).await;

            for (schema_id, documents) in documents {
                paginated_query_meta_fields_only(&client, &schema_id, documents.len()).await;
            };
        });
    }
}
