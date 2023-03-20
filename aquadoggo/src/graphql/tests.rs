// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for dynamic graphql schema generation and query resolution.
use std::convert::TryInto;

use async_graphql::{value, Response};
use p2panda_rs::document::DocumentId;
use p2panda_rs::schema::FieldType;
use p2panda_rs::test_utils::fixtures::random_key_pair;
use rstest::rstest;
use serde_json::json;

use crate::test_utils::{add_document, add_schema, graphql_test_client, test_runner, TestNode};

// Test querying application documents with scalar fields (no relations) by document id and by view
// id.
#[test]
fn scalar_fields() {
    test_runner(|mut node: TestNode| async move {
        let key_pair = random_key_pair();

        // Add schema to node.
        let schema = add_schema(
            &mut node,
            "schema_name",
            vec![
                ("bool", FieldType::Boolean),
                ("float", FieldType::Float),
                ("int", FieldType::Integer),
                ("text", FieldType::String),
            ],
            &key_pair,
        )
        .await;

        // Publish document on node.
        let doc_fields = vec![
            ("bool", true.into()),
            ("float", (1.0).into()),
            ("int", 1.into()),
            ("text", "yes".into()),
        ]
        .try_into()
        .unwrap();
        let view_id = add_document(&mut node, schema.id(), doc_fields, &key_pair).await;

        // Configure and send test query.
        let client = graphql_test_client(&node).await;
        let query = format!(
            r#"{{
                scalarDoc: {type_name}(viewId: "{view_id}") {{
                    fields {{
                        bool,
                        float,
                        int,
                        text
                    }}
                }},
            }}"#,
            type_name = schema.id(),
            view_id = view_id
        );

        let response = client
            .post("/graphql")
            .json(&json!({
                "query": query,
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "scalarDoc": {
                "fields": {
                    "bool": true,
                    "float": 1.0,
                    "int": 1,
                    "text": "yes",
                }
            },
        });
        assert_eq!(response.data, expected_data);
    });
}

// Test querying application documents across a parent-child relation using different kinds of
// relation fields.
#[rstest]
fn relation_fields() {
    test_runner(|mut node: TestNode| async move {
        let key_pair = random_key_pair();

        // Add schemas to node.
        let child_schema = add_schema(
            &mut node,
            "child",
            vec![("it_works", FieldType::Boolean)],
            &key_pair,
        )
        .await;

        let parent_schema = add_schema(
            &mut node,
            "parent",
            vec![
                (
                    "by_relation",
                    FieldType::Relation(child_schema.id().clone()),
                ),
                (
                    "by_pinned_relation",
                    FieldType::PinnedRelation(child_schema.id().clone()),
                ),
                (
                    "by_relation_list",
                    FieldType::RelationList(child_schema.id().clone()),
                ),
                (
                    "by_pinned_relation_list",
                    FieldType::PinnedRelationList(child_schema.id().clone()),
                ),
            ],
            &key_pair,
        )
        .await;

        // Publish child document on node.
        let child_view_id = add_document(
            &mut node,
            child_schema.id(),
            vec![("it_works", true.into())].try_into().unwrap(),
            &key_pair,
        )
        .await;
        // There is only one operation so view id = doc id.
        let child_doc_id: DocumentId = child_view_id.to_string().parse().unwrap();

        // Publish parent document on node.
        let parent_fields = vec![
            ("by_relation", child_doc_id.clone().into()),
            ("by_pinned_relation", child_view_id.clone().into()),
            ("by_relation_list", vec![child_doc_id].into()),
            ("by_pinned_relation_list", vec![child_view_id].into()),
        ];

        let parent_view_id =
            add_document(&mut node, parent_schema.id(), parent_fields, &key_pair).await;

        // Configure and send test query.
        let client = graphql_test_client(&node).await;
        let query = format!(
            r#"{{
                result: {}(viewId: "{}") {{
                    fields {{
                        by_relation {{ fields {{ it_works }} }},
                        by_pinned_relation {{ fields {{ it_works }} }},
                        by_relation_list {{ fields {{ it_works }} }},
                        by_pinned_relation_list {{ fields {{ it_works }} }},
                    }}
                }}
            }}"#,
            parent_schema.id(),
            parent_view_id,
        );

        let response = client
            .post("/graphql")
            .json(&json!({
                "query": query,
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "result": {
                "fields": {
                    "by_relation": {
                        "fields": {
                            "it_works": true
                        }
                    },
                    "by_pinned_relation": {
                        "fields": {
                            "it_works": true
                        }
                    },
                    "by_relation_list": [{
                        "fields": {
                            "it_works": true
                        }
                    }],
                    "by_pinned_relation_list": [{
                        "fields": {
                            "it_works": true
                        }
                    }]
                }
            }
        });

        assert_eq!(response.data, expected_data,);
    });
}
