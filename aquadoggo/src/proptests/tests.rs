// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::Response;
use p2panda_rs::api::publish;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::schema::SchemaId;
use proptest::{prop_compose, proptest, strategy::Just};
use serde_json::json;

use crate::proptests::document_strategies::{documents_strategy, DocumentAST};
use crate::proptests::schema_strategies::{schema_strategy, SchemaAST};
use crate::proptests::utils::{convert_schema_ast, encode_document_ast};
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
    #[test]
    fn test_query((schema, documents) in schema_with_documents_strategy()) {
        // The proptest strategies for generating schema and deriving collections of documents for each injects
        // the raw AST types into the test. Here we convert these into p2panda `Entries`, `Operations` and `Schema`
        // which we can then use to populate a store and run queries against.

        let mut schemas = HashMap::new();
        let mut document_entries_and_operations = Vec::new();

        // Encode entries and operations for all generated schema, as well as converting into the p2panda `Schema` themselves.
        convert_schema_ast(&schema, &mut schemas);

        // For each derived document, encode entries and operations.
        for document in documents.iter() {
            encode_document_ast(document, &mut document_entries_and_operations);
        }

        // Some sanity checks
        assert!(schemas.len() > 0);
        assert!(documents.len() > 0);
        assert!(document_entries_and_operations.len() > 0);

        // Now we start up a test runner and inject a test node we can populate.
        test_runner(|node: TestNode| async move {
            // Add all schema to the schema provider.
            for (_, schema) in schemas.clone() {
                node.context.schema_provider.update(schema.clone()).await;
            };

            // Publish all document entries and operations to the node.
            for (entry, operation) in document_entries_and_operations {
                let plain_operation = decode_operation(&operation).unwrap();
                let schema = node.context.schema_provider.get(plain_operation.schema_id()).await.unwrap();
                let result = publish(&node.context.store, &schema, &entry,  &plain_operation, &operation).await;
                assert!(result.is_ok());
            }

            let query = |type_name: &SchemaId, args: &str| -> String {
                format!(
                    r#"{{
                    collection: all_{type_name}{args} {{
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

            for (_, schema) in schemas {
                // Configure and send test query.
                let client = graphql_test_client(&node).await;

                let response = client
                    .post("/graphql")
                    .json(&json!({"query": query(schema.id(), ""),}))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());

                let query_str = query(
                    schema.id(),
                    &format!("(orderDirection: ASC, orderBy: {0})", schema.fields().keys().first().unwrap())
                );

                let response = client
                    .post("/graphql")
                    .json(&json!({"query": query_str}))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());
            };
        });
    }
}
