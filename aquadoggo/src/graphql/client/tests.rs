// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for dynamic graphql api

use std::convert::TryInto;

use async_graphql::{value, Response};
use log::{debug, error};
use p2panda_rs::schema::FieldType;
use p2panda_rs::test_utils::fixtures::random_key_pair;
use rstest::rstest;
use serde_json::json;
use tokio::sync::broadcast;

use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
use crate::graphql::GraphQLSchemaManager;
use crate::http::{build_server, HttpServiceContext};
use crate::test_helpers::TestClient;

#[rstest]
fn new_test(#[from(test_db)] runner: TestDatabaseRunner) {
    runner.with_db_teardown(&|mut db: TestDatabase| async move {
        debug!("In test");

        let key_pair = random_key_pair();

        // Add schema to node
        let schema = db
            .add_schema(
                "schema_name",
                vec![("it_works", FieldType::Bool)],
                &key_pair,
            )
            .await;

        // Publish document on node
        let doc_fields = vec![("it_works", true.into())].try_into().unwrap();
        let view_id = db.add_document(&schema.id(), doc_fields, &key_pair).await;

        let (tx, _) = broadcast::channel(16);
        let manager =
            GraphQLSchemaManager::new(db.store, tx, db.context.schema_provider.clone()).await;
        let http_context = HttpServiceContext::new(manager);
        let client = TestClient::new(build_server(http_context));

        let query = format!(
            r#"{{
                {}(viewId: "{}") {{
                    fields {{
                        it_works
                    }}
                }}
            }}"#,
            schema.id().as_str(),
            view_id.as_str()
        );

        let response = client
            .post("/graphql")
            .json(&json!({
                "query": query,
            }))
            .send()
            .await;

        let response: Response = response.json().await;

        if response.errors.len() > 0 {
            error!("Error: {:#?}", response.errors)
        }

        assert_eq!(
            response.data,
            value!({
                schema.id().as_str(): {
                    "fields": {
                        "it_works": true,
                    }
                }
            })
        );
    });
}
