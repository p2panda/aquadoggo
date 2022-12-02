// SPDX-License-Identifier: AGPL-3.0-or-later

//! Test correct generation of output schema.
use async_graphql::{value, Response, Value};
use p2panda_rs::schema::{FieldType, SchemaId, SYSTEM_SCHEMAS};
use p2panda_rs::test_utils::fixtures::random_key_pair;
use rstest::rstest;
use serde_json::json;
use serial_test::serial;

use crate::db::stores::test_utils::{add_schema, test_db, TestDatabase, TestDatabaseRunner};
use crate::test_helpers::graphql_test_client;

#[rstest]
#[serial]
#[case(SYSTEM_SCHEMAS[0].id().to_string(), SYSTEM_SCHEMAS[0].description().to_string())]
#[case(SYSTEM_SCHEMAS[1].id().to_string(), SYSTEM_SCHEMAS[1].description().to_string())]
fn system_schema_container_type(
    #[from(test_db)] runner: TestDatabaseRunner,
    #[case] type_name: String,
    #[case] type_description: String,
) {
    runner.with_db_teardown(move |db: TestDatabase| async move {
        let client = graphql_test_client(&db).await;
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": format!(
                    r#"{{
                        __type(name: "{}") {{
                            kind,
                            name,
                            description,
                            fields {{
                                name,
                                type {{
                                    name
                                }}
                            }}
                        }}
                    }}"#,
                    type_name
                ),
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "__type": {
                // Currently, all system schemas are object types.
                "kind": "OBJECT",
                "name": type_name,
                "description": type_description,
                "fields": [{
                    "name": "meta",
                    "type": {
                        "name": "DocumentMeta"
                    }
                },
                {
                    "name": "fields",
                    "type": {
                        "name": format!("{}Fields", type_name)
                    }
                }]
            }
        });

        assert_eq!(response.data, expected_data, "\n{:#?}\n", response.errors);
    });
}

#[rstest]
#[serial]
fn application_schema_container_type(#[from(test_db)] runner: TestDatabaseRunner) {
    runner.with_db_teardown(move |mut db: TestDatabase| async move {
        let key_pair = random_key_pair();

        // Add schema to test database.
        let schema = add_schema(
            &mut db,
            "schema_name",
            vec![("bool_field", FieldType::Boolean)],
            &key_pair,
        )
        .await;

        let type_name = schema.id().to_string();

        let client = graphql_test_client(&db).await;
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": format!(
                    r#"{{
                        schema: __type(name: "{}") {{
                            kind,
                            name,
                            description,
                            fields {{
                                name,
                                type {{
                                    name
                                }}
                            }}
                        }},
                    }}"#,
                    type_name,
                ),
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "schema": {
                "kind": "OBJECT",
                "name": type_name,
                "description": schema.description(),
                "fields": [{
                    "name": "meta",
                    "type": {
                        "name": "DocumentMeta"
                    }
                },
                {
                    "name": "fields",
                    "type": {
                        "name": format!("{}Fields", type_name)
                    }
                }]
            },
        });

        assert_eq!(response.data, expected_data, "\n{:#?}\n", response.errors);
    });
}

#[rstest]
#[serial]
fn application_schema_fields_type(#[from(test_db)] runner: TestDatabaseRunner) {
    runner.with_db_teardown(move |mut db: TestDatabase| async move {
        let key_pair = random_key_pair();

        // Add schema to node.
        let schema = add_schema(
            &mut db,
            "schema_name",
            vec![
                // scalar field
                ("bool_field", FieldType::Boolean),
                // object field
                (
                    "relation_field",
                    FieldType::Relation(SchemaId::SchemaDefinition(1)),
                ),
                // list field
                (
                    "list_field",
                    FieldType::RelationList(SchemaId::SchemaDefinition(1)),
                ),
            ],
            &key_pair,
        )
        .await;

        let type_name = schema.id().to_string();

        let client = graphql_test_client(&db).await;
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": format!(
                    r#"{{
                        schemaFields: __type(name: "{}Fields") {{
                            description,
                            fields {{
                                name,
                                type {{
                                    kind,
                                    name
                                }}
                            }}
                        }}
                    }}"#,
                    type_name,
                ),
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "schemaFields": {
                "description": "Data fields available on documents of this schema.",
                "fields": [{
                    "name": "bool_field",
                    "type": {
                        "kind": "SCALAR",
                        "name": "Boolean"
                    }
                },{
                    "name": "list_field",
                    "type": {
                        "kind": "LIST",
                        "name": Value::Null
                    }
                },{
                    "name": "relation_field",
                    "type": {
                        "kind": "OBJECT",
                        "name": "schema_definition_v1"
                    }
                }]
            }
        });

        assert_eq!(response.data, expected_data, "\n{:#?}\n", response.errors);
    });
}

#[rstest]
#[serial]
fn metadata_type(#[from(test_db)] runner: TestDatabaseRunner) {
    runner.with_db_teardown(move |db: TestDatabase| async move {
        let client = graphql_test_client(&db).await;
        let response = client
            .post("/graphql")
            .json(&json!({
                "query": r#"{
                        __type(name: "DocumentMeta") {
                            kind,
                            name,
                            description,
                            fields {
                                name,
                                type {
                                    name
                                }
                            }
                        }
                    }"#,
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        let expected_data = value!({
            "__type": {
                // Currently, all system schemas are object types.
                "kind": "OBJECT",
                "name": "DocumentMeta",
                "description": "Metadata for documents of this schema.",
                "fields": [{
                    "name": "documentId",
                    "type": {
                        "name": "DocumentId"
                    }
                },
                {
                    "name": "viewId",
                    "type": {
                        "name": "DocumentViewId"
                    }
                }]
            }
        });

        assert_eq!(response.data, expected_data, "\n{:#?}\n", response.errors);
    });
}
