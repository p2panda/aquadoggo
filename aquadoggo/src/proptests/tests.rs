// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::{Response, Value};
use p2panda_rs::schema::SchemaId;
use proptest::test_runner::Config;
use proptest::{prop_compose, proptest, strategy::Just};
use serde_json::json;

use crate::proptests::document_strategies::{documents_strategy, DocumentAST};
use crate::proptests::schema_strategies::{schema_strategy, SchemaAST};
use crate::proptests::utils::{add_documents_from_ast, add_schemas_from_ast};
use crate::test_utils::{graphql_test_client, test_runner, TestNode};

prop_compose! {
    fn schema_with_documents_strategy()
            (schema in schema_strategy())
            (documents in documents_strategy(schema.clone()), schema in Just(schema))
            -> (SchemaAST, Vec<DocumentAST>) {
        (schema, documents)
    }
}

proptest! {
    #![proptest_config(Config {
        cases: 20, .. Config::default()
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

            // Sanity checks
            assert!(documents.len() > 0);


            // Helper for creating queries
            let query = |type_name: &SchemaId, args: &str| -> String {
                format!(
                    r#"{{
                    query: all_{type_name}{args} {{
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

            for (schema_id, documents) in documents {
                let total_documents = documents.len();
                let schema = node.context.schema_provider.get(&schema_id).await.unwrap();

                // Configure and send test query.
                let client = graphql_test_client(&node).await;

                let response = client
                    .post("/graphql")
                    .json(&json!({"query": query(&schema_id, "")}))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());
                let documents = get_documents_from_response(&response);
                assert_eq!(documents.len(), total_documents);

                let query_str = query(
                    &schema_id,
                    &format!("(orderDirection: ASC, orderBy: {0})", schema.fields().keys().first().unwrap())
                );

                let response = client
                    .post("/graphql")
                    .json(&json!({"query": query_str}))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());
                let documents = get_documents_from_response(&response);
                assert_eq!(documents.len(), total_documents);
            };
        });
    }
}

fn get_documents_from_response(response: &Response) -> Vec<Value> {
    let response = match &response.data {
        async_graphql::Value::Object(response) => response.get("query").unwrap().to_owned(),
        _ => panic!("Expected object in response"),
    };

    let documents = match &response {
        async_graphql::Value::Object(response) => response.get("documents").unwrap().to_owned(),
        _ => panic!("Expected object in response"),
    };

    match documents {
        async_graphql::Value::List(collection) => collection.into_iter().map(Value::from).collect(),
        _ => panic!("Expected list in response"),
    }
}
