// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result};
use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::Author;
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::provider::SqlStorage;
use crate::db::Pool;

use super::{EntryArgsRequest, EntryArgsResponse};

#[derive(Default, Debug, Copy, Clone)]
/// The GraphQL root for the client api that p2panda clients can use to connect to a node.
pub struct Query;

#[Object]
impl Query {
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

        // Prepare database connection
        let pool = ctx.data::<Pool>()?;
        let provider = SqlStorage {
            pool: pool.to_owned(),
        };

        provider.get_entry_args(&args).await.map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::Response;
    use p2panda_rs::entry::{LogId, SeqNum};
    use serde_json::json;

    use crate::config::Configuration;
    use crate::context::Context;
    use crate::graphql::client::EntryArgsResponse;
    use crate::server::build_server;
    use crate::test_helpers::{initialize_db, TestClient};

    #[tokio::test]
    async fn next_entry_args_valid_query() {
        let pool = initialize_db().await;
        let context = Context::new(pool.clone(), Configuration::default());
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
            // .json::<GQLResponse<EntryArgsGQLResponse>>()
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
        let pool = initialize_db().await;
        let context = Context::new(pool.clone(), Configuration::default());
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
