// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::Author;

use crate::db::provider::SqlStorage;
use crate::domain::next_args;
use crate::graphql::client::response::NextEntryArguments;
use crate::graphql::scalars;

/// GraphQL queries for the Client API.
#[derive(Default, Debug, Copy, Clone)]
pub struct ClientRoot;

#[Object]
impl ClientRoot {
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
        document_view_id: Option<scalars::DocumentViewId>,
    ) -> Result<NextEntryArguments> {
        // Access the store from context.
        let store = ctx.data::<SqlStorage>()?;

        let public_key: Author = public_key.into();
        let document_view_id = document_view_id.map(DocumentViewId::from);

        next_args(store, &public_key, document_view_id.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use rstest::rstest;
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::http::build_server;
    use crate::http::HttpServiceContext;
    use crate::test_helpers::TestClient;

    #[rstest]
    fn next_entry_args_valid_query(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let (tx, _) = broadcast::channel(16);
            let context = HttpServiceContext::new(db.store, tx);
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
                        "logId": "0",
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
            let context = HttpServiceContext::new(db.store, tx);
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
