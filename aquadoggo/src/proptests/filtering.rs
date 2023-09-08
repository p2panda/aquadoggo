// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use proptest::test_runner::Config;
use proptest::{prop_compose, proptest, strategy::Just};

use crate::proptests::queries::{make_query, paginated_query_application_fields};
use crate::proptests::strategies::{
    application_filters_strategy, documents_strategy, meta_field_filter_strategy, schema_strategy,
    DocumentAST, FieldName, Filter, MetaField, SchemaAST,
};
use crate::proptests::utils::{
    add_documents_from_ast, add_schemas_from_ast, parse_filter, parse_selected_fields,
    sanity_checks,
};
use crate::test_utils::{graphql_test_client, test_runner, TestNode};

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
    #![proptest_config(Config::with_cases(100))]
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
