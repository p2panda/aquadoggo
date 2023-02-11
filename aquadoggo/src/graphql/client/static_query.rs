// SPDX-License-Identifier: AGPL-3.0-or-later

//! Static fields of the client api.
use async_graphql::{Context, Object, Result};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::PublicKey;

use crate::db::SqlStore;
use crate::domain::next_args;
use crate::graphql::client::NextArguments;
use crate::graphql::scalars;

/// GraphQL queries for the Client API.
#[derive(Default, Debug, Copy, Clone)]
pub struct StaticQuery;

#[Object]
impl StaticQuery {
    /// Return required arguments for publishing the next entry.
    async fn next_args(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            name = "publicKey",
            desc = "Public key of author that will encode and sign the next entry \
            using the returned arguments"
        )]
        public_key: scalars::PublicKeyScalar,
        #[graphql(
            name = "viewId",
            desc = "Document the entry's UPDATE or DELETE operation is referring to, \
            can be left empty when it is a CREATE operation"
        )]
        document_view_id: Option<scalars::DocumentViewIdScalar>,
    ) -> Result<NextArguments> {
        // Access the store from context.
        let store = ctx.data::<SqlStore>()?;

        // Convert and validate passed parameters.
        let public_key: PublicKey = public_key.into();
        let document_view_id = document_view_id.map(|val| DocumentViewId::from(&val));

        // Calculate next entry's arguments.
        next_args(store, &public_key, document_view_id.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use rstest::rstest;
    use serde_json::json;
    use serial_test::serial;

    use crate::test_utils::graphql_test_client;
    use crate::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    // Note: This and more tests in this file use the underlying static schema provider which is a
    // static mutable data store, accessible across all test runner threads in parallel mode. To
    // prevent overwriting data across threads we have to run this test in serial.
    //
    // Read more: https://users.rust-lang.org/t/static-mutables-in-tests/49321
    #[serial]
    fn next_args_valid_query(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
                .post("/graphql")
                .json(&json!({
                    "query": r#"{
                        nextArgs(
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
                    "nextArgs": {
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
    #[serial] // See note above on why we execute this test in series
    fn next_args_valid_query_with_document_id(
        #[with(1, 1, 1)]
        #[from(test_db)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
            let document_id = db.test_data.documents.get(0).unwrap();
            let public_key =
                db.test_data.key_pairs[0].public_key();

            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
                .post("/graphql")
                .json(&json!({
                    "query":
                        format!("{{
                            nextArgs(
                                publicKey: \"{}\",
                                viewId: \"{}\"
                            ) {{
                                logId,
                                seqNum,
                                backlink,
                                skiplink
                            }}
                        }}",
                            public_key,
                            document_id.as_str()
                        )
                }))
                .send()
                .await
                .json::<Response>()
                .await;

            assert!(received_entry_args.is_ok());
            assert_eq!(
                received_entry_args.data,
                value!({
                    "nextArgs": {
                        "logId": "0",
                        "seqNum": "2",
                        "backlink": "00203c56166a80316aec6b629814ffbafb6bf54d9e30093e122b3cb0f7220e82f15d",
                        "skiplink": null,
                    }
                })
            );
        })
    }

    #[rstest]
    #[serial] // See note above on why we execute this test in series
    fn next_args_error_response(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
            let response = client
                .post("/graphql")
                .json(&json!({
                    "query": r#"{
                    nextArgs(publicKey: "nope") {
                        logId
                    }
                }"#,
                }))
                .send()
                .await;

            let response: Response = response.json().await;
            assert_eq!(
                response.errors[0].message,
                "Failed to parse \"PublicKey\": invalid hex encoding in public key string"
            )
        })
    }
}
