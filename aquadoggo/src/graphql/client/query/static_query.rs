// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::document::{Document, DocumentBuilder, DocumentId};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsVerifiedOperation, Operation, OperationFields, OperationValue, VerifiedOperation,
};
use p2panda_rs::storage_provider::traits::StorageProvider;
use p2panda_rs::Validate;

use crate::db::provider::SqlStorage;
use crate::db::request::EntryArgsRequest;
use crate::graphql::client::response::{DocumentResponse, NextEntryArguments};
use crate::graphql::scalars;

/// GraphQL queries for the Client API.
#[derive(Default, Debug, Copy, Clone)]
pub struct StaticQuery;

#[Object]
impl StaticQuery {
    /// Return required arguments for publishing the next entry.
    async fn next_entry_args(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            name = "publicKey",
            desc = "Public key of author that will encode and sign the next entry \
            using the returned arguments"
        )]
        public_key: scalars::PublicKey,
        #[graphql(
            name = "documentId",
            desc = "Document the entry's UPDATE or DELETE operation is referring to, \
            can be left empty when it is a CREATE operation"
        )]
        document_id: Option<scalars::DocumentId>,
    ) -> Result<NextEntryArguments> {
        let args = EntryArgsRequest {
            public_key: public_key.into(),
            document_id: document_id.map(DocumentId::from),
        };
        args.validate()?;

        // Load and return next entry arguments
        let store = ctx.data::<SqlStorage>()?;
        store.get_entry_args(&args).await.map_err(Error::from)
    }

    // @TODO
    async fn document(&self, _ctx: &Context<'_>, document: String) -> Result<DocumentResponse> {
        let document_id = document.parse::<DocumentId>()?;

        let document = get_document_by_id(document_id);

        Ok(DocumentResponse {
            view: document.view().map(|view| view.to_owned()),
        })
    }
}

// @TODO
fn get_document_by_id(_document: DocumentId) -> Document {
    let schema = "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        .parse()
        .unwrap();
    let mut fields = OperationFields::new();
    fields
        .add("name", OperationValue::Text("test".to_string()))
        .unwrap();
    let public_key =
        Author::new("7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982").unwrap();
    let create_operation = Operation::new_create(schema, fields).unwrap();
    let operation_id = "00201c221b573b1e0c67c5e2c624a93419774cdf46b3d62414c44a698df1237b1c16"
        .parse()
        .unwrap();
    let verified_operation =
        VerifiedOperation::new(&public_key, &operation_id, &create_operation).unwrap();
    DocumentBuilder::new(vec![verified_operation])
        .build()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use rstest::rstest;
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::graphql::GraphQLSchemaManager;
    use crate::http::{build_server, HttpServiceContext};
    use crate::schema::SchemaProvider;
    use crate::test_helpers::TestClient;

    #[rstest]
    fn next_entry_args_valid_query(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _) = broadcast::channel(16);
            let schema_provider = SchemaProvider::default();
            let manager = GraphQLSchemaManager::new(db.store, tx, schema_provider).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
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

            assert_eq!(
                received_entry_args.data,
                value!({
                    "nextEntryArgs": {
                        "logId": "1",
                        "seqNum": "1",
                        "backlink": null,
                        "skiplink": null,
                    }
                })
            );
        })
    }

    #[rstest]
    fn next_entry_args_error_response(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _) = broadcast::channel(16);
            let schema_provider = SchemaProvider::default();
            let manager = GraphQLSchemaManager::new(db.store, tx, schema_provider).await;
            let context = HttpServiceContext::new(manager);
            let client = TestClient::new(build_server(context));

            // Selected fields need to be alphabetically sorted because that's what the `json` macro
            // that is used in the assert below produces.
            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": r#"{
                    nextEntryArgs(publicKey: "nope") {
                        logId
                    }
                }"#,
                }))
                .send()
                .await;

            let response: Response = response.json().await;
            assert_eq!(
                response.errors[0].message,
                "invalid hex encoding in author string"
            )
        })
    }
}
