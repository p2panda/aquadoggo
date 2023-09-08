// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use proptest::test_runner::Config;
use proptest::{prop_compose, proptest, strategy::Just};

use crate::proptests::queries::{
    paginated_query, paginated_query_application_fields, paginated_query_meta_fields_only,
};
use crate::proptests::strategies::{documents_strategy, schema_strategy, DocumentAST, SchemaAST};
use crate::proptests::utils::{
    add_documents_from_ast, add_schemas_from_ast, parse_selected_fields, sanity_checks,
};
use crate::test_utils::{http_test_client, test_runner, TestNode};

prop_compose! {
    /// Strategy for generating schemas, and documents from these schema.
    fn schema_with_documents_strategy()
            (schema in schema_strategy())
            (documents in documents_strategy(schema.clone()), schema in Just(schema))
            -> (SchemaAST, Vec<DocumentAST>) {
        (schema, documents)
    }
}

proptest! {
    #![proptest_config(Config::with_cases(100))]
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
            let client = http_test_client(&node).await;

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
}
