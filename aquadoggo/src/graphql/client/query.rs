// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::identity::Author;
use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, LogStore};
use p2panda_rs::Validate;

use crate::db::provider::SqlStorage;
use crate::graphql::client::response::NextEntryArguments;
use crate::graphql::scalars;
use crate::validation::get_validate_document_id_for_view_id;

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

        // Validate the public key.
        let public_key: Author = public_key.into();
        public_key.validate()?;

        let document_view_id = document_view_id.map(DocumentViewId::from);

        // If no document_view_id is passed then this is a request for publishing a CREATE operation
        // and we return the args for the next free log by this author.
        if document_view_id.is_none() {
            let log_id = store.next_log_id(&public_key).await?;
            return Ok(NextEntryArguments {
                backlink: None,
                skiplink: None,
                seq_num: SeqNum::default().into(),
                log_id: log_id.into(),
            });
        }

        // Access and validate the document view id.
        //
        // We can unwrap here as we know document_view_id is some.
        let document_view_id: DocumentViewId = document_view_id.unwrap();
        document_view_id.validate()?;

        // Get the document_id for this document_view_id. This performs several validation steps (check
        // method doc string).
        let document_id = get_validate_document_id_for_view_id(&store, &document_view_id).await?;

        // Retrieve the log_id for the found document_id and author.
        //
        // (lolz, this method is just called `get()`)
        let log_id = match store.get(&public_key, &document_id).await? {
            // This public key already wrote to this document, so we return the found log_id
            Some(log_id) => log_id,
            // This public_key never wrote to this document before so we return a new log_id
            None => store.next_log_id(&public_key).await?,
        };

        // Get the backlink by getting the latest entry in this log.
        let backlink = store
            .get_latest_entry(&public_key, &log_id)
            .await?
            .expect("All logs must contain at least one entry");

        // Determine skiplink ("lipmaa"-link) entry in this log.
        let skiplink_hash = store.determine_next_skiplink(&backlink).await?;

        // Determine the next sequence number.
        let seq_num = backlink
            .seq_num()
            .next()
            .expect("Max sequence number reached \\*o*/");

        Ok(NextEntryArguments {
            backlink: Some(backlink.hash().to_owned().into()),
            skiplink: skiplink_hash.map(|hash| hash.into()),
            seq_num: seq_num.into(),
            log_id: log_id.into(),
        })
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
