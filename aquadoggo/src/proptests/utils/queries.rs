// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Response;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::SchemaId;
use serde_json::{json, Value as JsonValue};

use crate::test_utils::TestClient;

/// Make a GraphQL query to the node.
pub async fn make_query(client: &TestClient, query_str: &str) -> JsonValue {
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
pub fn paginated_query_meta_fields_only(
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
pub fn paginated_query_application_fields(
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
pub async fn paginated_query(
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
