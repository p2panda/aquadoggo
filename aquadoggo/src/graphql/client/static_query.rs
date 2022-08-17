// SPDX-License-Identifier: AGPL-3.0-or-later

//! Static fields of the client api.
use async_graphql::{Context, Object, Result};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::identity::Author;
use p2panda_rs::Validate;

use crate::db::provider::SqlStorage;
use crate::domain::next_args;
use crate::graphql::client::NextEntryArguments;
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
        public_key: scalars::PublicKeyScalar,
        #[graphql(
            name = "documentId",
            desc = "Document the entry's UPDATE or DELETE operation is referring to, \
            can be left empty when it is a CREATE operation"
        )]
        document_id: Option<scalars::DocumentIdScalar>,
    ) -> Result<NextEntryArguments> {
        // @TODO: The api for `next_entry_args` needs to be updated to accept a `DocumentViewId`

        // Access the store from context.
        let store = ctx.data::<SqlStorage>()?;

        // Convert and validate passed parameters.
        let public_key: Author = public_key.into();
        let document_id = document_id.map(|val| DocumentId::from(&val));

        public_key.validate()?;
        if let Some(ref document_view_id) = document_id {
            document_view_id.validate()?;
        }

        // Convert document_id into document_view_id and unwrap as we already validated above.
        let document_view_id: Option<DocumentViewId> =
            document_id.map(|id| id.as_str().parse().unwrap());

        // Calculate next entry args.
        next_args(store, &public_key, document_view_id.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, Response};
    use p2panda_rs::identity::Author;
    use rstest::rstest;
    use serde_json::json;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::test_helpers::{graphql_test_client};

    #[rstest]
    fn next_entry_args_valid_query(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
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
    fn next_entry_args_valid_query_with_document_id(
        #[with(1, 1, 1)]
        #[from(test_db)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
            let document_id = db.test_data.documents.get(0).unwrap();
            let author =
                Author::from(db.test_data.key_pairs[0].public_key());

            // Selected fields need to be alphabetically sorted because that's what the `json`
            // macro that is used in the assert below produces.
            let received_entry_args = client
                .post("/graphql")
                .json(&json!({
                    "query":
                        format!(
                            "{{
                        nextEntryArgs(
                            publicKey: \"{}\",
                            documentId: \"{}\"
                        ) {{
                            logId,
                            seqNum,
                            backlink,
                            skiplink
                        }}
                    }}",
                            author.as_str(),
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
                    "nextEntryArgs": {
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
    fn next_entry_args_error_response(#[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            let client = graphql_test_client(&db).await;
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
                "Failed to parse \"PublicKeyScalar\": invalid hex encoding in author string"
            )
        })
    }
}
