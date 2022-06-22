// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::document::{Document, DocumentBuilder, DocumentId};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{Operation, OperationFields, OperationValue, VerifiedOperation};
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::provider::SqlStorage;
use crate::graphql::client::response::DocumentResponse;
use crate::graphql::client::{EntryArgsRequest, EntryArgsResponse};

/// The GraphQL root for the client api that p2panda clients can use to connect to a node.
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
            desc = "Public key that will publish using the returned entry arguments"
        )]
        public_key_param: String,
        #[graphql(
            name = "documentId",
            desc = "Document id to which the entry's operation will apply"
        )]
        document_id_param: Option<String>,
    ) -> Result<EntryArgsResponse> {
        // Parse and validate parameters
        let document_id = match document_id_param {
            Some(val) => Some(val.parse::<DocumentId>()?),
            None => None,
        };
        let args = EntryArgsRequest {
            author: Author::new(&public_key_param)?,
            document: document_id,
        };

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
    use async_graphql::Response;
    use p2panda_rs::entry::{LogId, SeqNum};
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::graphql::client::EntryArgsResponse;
    use crate::graphql::GraphQLSchemaManager;
    use crate::http::build_server;
    use crate::http::HttpServiceContext;
    use crate::schema_service::SchemaService;
    use crate::test_helpers::{initialize_store, TestClient};

    #[tokio::test]
    async fn next_entry_args_valid_query() {
        let (tx, _) = broadcast::channel(16);
        let store = initialize_store().await;
        let schema_service = SchemaService::new(store.clone());
        let manager = GraphQLSchemaManager::new(store, tx, schema_service).await;
        let context = HttpServiceContext::new(manager);
        let client = TestClient::new(build_server(context));

        // Selected fields need to be alphabetically sorted because that's what the `json` macro
        // that is used in the assert below produces.
        let response = client
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

        let expected_entry_args = EntryArgsResponse {
            log_id: LogId::new(1),
            seq_num: SeqNum::new(1).unwrap(),
            backlink: None,
            skiplink: None,
        };
        let received_entry_args: EntryArgsResponse = match response.data {
            async_graphql::Value::Object(result_outer) => {
                async_graphql::from_value(result_outer.get("nextEntryArgs").unwrap().to_owned())
                    .unwrap()
            }
            _ => panic!("Expected return value to be an object"),
        };

        assert_eq!(received_entry_args, expected_entry_args);
    }

    #[tokio::test]
    async fn next_entry_args_error_response() {
        let (tx, _) = broadcast::channel(16);
        let store = initialize_store().await;
        let schema_service = SchemaService::new(store.clone());
        let manager = GraphQLSchemaManager::new(store, tx, schema_service).await;
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
    }
}
