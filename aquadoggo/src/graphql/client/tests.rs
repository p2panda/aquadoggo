// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for dynamic graphql schema generation and query resolution.
use std::convert::TryInto;
use std::str::FromStr;

use async_graphql::{value, Response};
use async_graphql::{Request, Variables};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::sign_and_encode_entry;
use p2panda_rs::entry::Entry;
use p2panda_rs::entry::EntryBuilder;
use p2panda_rs::entry::LogId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::OperationBuilder;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::operation::{Operation, OperationAction};
use p2panda_rs::schema::FieldType;
use p2panda_rs::schema::Schema;
use p2panda_rs::test_utils::fixtures::random_key_pair;
use p2panda_rs::test_utils::fixtures::schema;
use rstest::rstest;
use serde_json::json;

use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
use crate::test_helpers::graphql_test_client;
use crate::test_helpers::TestClient;

#[rstest]
fn scalar_fields(#[from(test_db)] runner: TestDatabaseRunner) {
    // Test querying application documents with scalar fields (no relations) by document id and by
    // view id.

    runner.with_db_teardown(&|mut db: TestDatabase| async move {
        let key_pair = random_key_pair();

        // Add schema to node.
        let schema = db
            .add_schema(
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
        let view_id = db.add_document(schema.id(), doc_fields, &key_pair).await;

        // Configure and send test query.
        let client = graphql_test_client(&db).await;
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

#[rstest]
fn relation_fields(#[from(test_db)] runner: TestDatabaseRunner) {
    // Test querying application documents across a parent-child relation using different kinds of
    // relation fields.

    runner.with_db_teardown(&|mut db: TestDatabase| async move {
        let key_pair = random_key_pair();

        // Add schemas to node.
        let child_schema = db
            .add_schema("child", vec![("it_works", FieldType::Boolean)], &key_pair)
            .await;

        let parent_schema = db
            .add_schema(
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
        let child_view_id = db
            .add_document(
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

        let parent_view_id = db
            .add_document(parent_schema.id(), parent_fields, &key_pair)
            .await;

        // Configure and send test query.
        let client = graphql_test_client(&db).await;
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

async fn next_args(
    client: &TestClient,
    author: &Author,
    document_id: Option<&DocumentId>,
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    let result = client
        .post("/graphql")
        .json(&json!({
            "query": r#"{
                nextEntryArgs(
                    publicKey: "8b52ae153142288402382fd6d9619e018978e015e6bc372b1b0c7bd40c6a240a"
                ) {
                    logId,
                    seqNum,
                    backlink,
                    skiplink
                }
            }"#,
        }))
        .send()
        .await
        .json::<Response>()
        .await;

    let json_value = result.data.into_json().unwrap();
    let log_id = LogId::from_str(json_value["nextEntryArgs"]["logId"].as_str().unwrap()).unwrap();
    let seq_num =
        SeqNum::from_str(json_value["nextEntryArgs"]["seqNum"].as_str().unwrap()).unwrap();
    let backlink = json_value["nextEntryArgs"]["backlink"]
        .as_str()
        .map(|hash| Hash::from_str(hash).unwrap());
    let skiplink = json_value["nextEntryArgs"]["skiplink"]
        .as_str()
        .map(|hash| Hash::from_str(hash).unwrap());
    (log_id, seq_num, backlink, skiplink)
}

async fn publish(
    client: &TestClient,
    key_pair: &KeyPair,
    operation: &Operation,
    next_args: &(LogId, SeqNum, Option<Hash>, Option<Hash>),
) -> (LogId, SeqNum, Option<Hash>, Option<Hash>) {
    let encoded_operation = encode_operation(&operation).expect("Encode operation");
    let (log_id, seq_num, backlink, skiplink) = next_args;

    let encoded_entry = sign_and_encode_entry(
        &log_id,
        &seq_num,
        skiplink.as_ref(),
        backlink.as_ref(),
        &encoded_operation,
        &key_pair,
    )
    .expect("Encode entry");

    // Prepare GraphQL mutation publishing an entry
    let parameters = Variables::from_value(value!({
        "entry": encoded_entry.to_string(),
        "operation": encoded_operation.to_string(),
    }));

    let request = Request::new(
        r#"
    mutation TestPublishEntry($entry: String!, $operation: String!) {
        publishEntry(entry: $entry, operation: $operation) {
            logId,
            seqNum,
            backlink,
            skiplink
        }
    }"#,
    )
    .variables(parameters);

    let response = client
        .post("/graphql")
        .json(&json!({
          "query": request.query,
          "variables": request.variables
        }
        ))
        .send()
        .await
        .json::<Response>()
        .await;

    let json_value = response.data.into_json().unwrap();
    let log_id = LogId::from_str(json_value["publishEntry"]["logId"].as_str().unwrap()).unwrap();
    let seq_num = SeqNum::from_str(json_value["publishEntry"]["seqNum"].as_str().unwrap()).unwrap();
    let backlink = json_value["publishEntry"]["backlink"]
        .as_str()
        .map(|hash| Hash::from_str(hash).unwrap());
    let skiplink = json_value["publishEntry"]["skiplink"]
        .as_str()
        .map(|hash| Hash::from_str(hash).unwrap());
    (log_id, seq_num, backlink, skiplink)
}

#[rstest]
fn e2e_publish_test(#[from(test_db)] runner: TestDatabaseRunner) {
    runner.with_db_teardown(|mut db: TestDatabase| async move {
        let schema = db
            .add_schema("cafe", vec![("name", FieldType::String)], &KeyPair::new())
            .await;

        let panda = KeyPair::from_private_key_str(
            "ddcafe34db2625af34c8ba3cf35d46e23283d908c9848c8b43d1f5d0fde779ea",
        )
        .unwrap();

        let penguin = KeyPair::from_private_key_str(
            "1c86b2524b48f0ba86103cddc6bdfd87774ab77ab4c0ea989ed0eeab3d28827a",
        )
        .unwrap();

        let client = graphql_test_client(&db).await;

        // Panda publishes a CREATE operation.
        // This instantiates a new document.
        //
        // DOCUMENT: [panda_1]

        let panda_next_args = next_args(&client, &Author::from(panda.public_key()), None).await;
        let panda_operation_1 = OperationBuilder::new(schema.id())
            .action(OperationAction::Create)
            .fields(&[("name", OperationValue::String("Panda Cafe".to_string()))])
            .build()
            .unwrap();

        let panda_next_args = publish(&client, &panda, &panda_operation_1, &panda_next_args).await;
        let panda_entry_1_hash = panda_next_args.clone().2.unwrap();

        // Panda publishes an UPDATE operation.
        // It contains the id of the previous operation in it's `previous_operations` array
        //
        // DOCUMENT: [panda_1]<--[panda_2]

        let panda_operation_2 = OperationBuilder::new(schema.id())
            .action(OperationAction::Update)
            .fields(&[("name", OperationValue::String("Panda Cafe!".to_string()))])
            .previous_operations(&panda_entry_1_hash.clone().into())
            .build()
            .unwrap();

        let panda_next_args = publish(&client, &panda, &panda_operation_2, &panda_next_args).await;
        let panda_entry_2_hash = panda_next_args.clone().2.unwrap();

        // Penguin publishes an update operation which creates a new branch in the graph.
        // This is because they didn't know about Panda's second operation.
        //
        // DOCUMENT: [panda_1]<--[penguin_1]
        //                    \----[panda_2]

        let penguin_operation_1 = OperationBuilder::new(schema.id())
            .action(OperationAction::Update)
            .fields(&[(
                "name",
                OperationValue::String("Penguin Cafe!!!".to_string()),
            )])
            .previous_operations(&panda_entry_1_hash.clone().into())
            .build()
            .unwrap();

        let penguin_next_args = next_args(&client, &Author::from(penguin.public_key()), None).await;
        let penguin_next_args =
            publish(&client, &penguin, &penguin_operation_1, &penguin_next_args).await;
        let penguin_entry_1_hash = penguin_next_args.clone().2.unwrap();

        // Penguin publishes a new operation while now being aware of the previous branching situation.
        // Their `previous_operations` field now contains 2 operation id's.
        //
        // DOCUMENT: [panda_1]<--[penguin_1]<---[penguin_2]
        //                    \----[panda_2]<--/

        let penguin_operation_2 = OperationBuilder::new(schema.id())
            .action(OperationAction::Update)
            .fields(&[(
                "name",
                OperationValue::String("Polar Bear Cafe".to_string()),
            )])
            .previous_operations(&DocumentViewId::new(&[
                penguin_entry_1_hash.into(),
                panda_entry_2_hash.into(),
            ]))
            .build()
            .unwrap();

        let penguin_next_args =
            publish(&client, &penguin, &penguin_operation_2, &penguin_next_args).await;
        let penguin_entry_2_hash = penguin_next_args.clone().2.unwrap();

        // Penguin publishes a new update operation which points at the current graph tip.
        //
        // DOCUMENT: [panda_1]<--[penguin_1]<---[penguin_2]<--[penguin_3]
        //                    \----[panda_2]<--/

        let penguin_operation_3 = OperationBuilder::new(schema.id())
            .action(OperationAction::Update)
            .fields(&[(
                "name",
                OperationValue::String("Polar Bear Cafe!!!!!!!!!!".to_string()),
            )])
            .previous_operations(&penguin_entry_2_hash.into())
            .build()
            .unwrap();

        let penguin_next_args =
            publish(&client, &penguin, &penguin_operation_3, &penguin_next_args).await;
        let _penguin_entry_3_hash = penguin_next_args.clone().2.unwrap();
    })
}
