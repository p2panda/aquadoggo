// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Error, Object, Result, SimpleObject};
use bamboo_rs_core_ed25519_yasmf::entry::is_lipmaa_required as is_skiplink_required;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;

use crate::db::models::{Entry, Log};
use crate::db::Pool;

#[derive(SimpleObject)]
pub struct EntryArgs {
    pub log_id: String,
    pub seq_num: String,
    pub backlink: Option<String>,
    pub skiplink: Option<String>,
}

#[derive(Default, Debug, Copy, Clone)]
/// The GraphQL root for the client api that p2panda clients can use to connect to a node.
pub struct ClientRoot;

#[Object]
impl ClientRoot {
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
    ) -> Result<EntryArgs> {
        let public_key = Author::new(&public_key_param)?;
        let document_id = match document_id_param {
            Some(value) => Some(Hash::new(&value)?),
            None => None,
        };

        // Get database connection pool
        let pool = ctx.data::<Pool>().map_err(|err| Error::from(err))?;

        // Determine log_id for this document. If this is the very first operation in the document
        // graph, the `document` value is None and we will return the next free log id
        let log_id = Log::find_document_log_id(&pool, &public_key, document_id.as_ref()).await?;

        // Determine backlink and skiplink hashes for the next entry. To do this we need the latest
        // entry in this log
        let entry_latest = Entry::latest(&pool, &public_key, &log_id).await?;

        match entry_latest {
            // An entry was found which serves as the backlink for the upcoming entry
            Some(mut entry_backlink) => {
                // Determine skiplink ("lipmaa"-link) entry in this log
                let skiplink = determine_skiplink(pool.clone(), &entry_backlink).await?;

                Ok(EntryArgs {
                    log_id: log_id.as_u64().to_string(),
                    seq_num: entry_backlink.seq_num.next().unwrap().as_u64().to_string(),
                    backlink: Some(entry_backlink.entry_hash.as_str().into()),
                    skiplink: skiplink.map(|h| h.as_str().into()),
                })
            }
            // No entry was given yet, we can assume this is the beginning of the log
            None => Ok(EntryArgs {
                log_id: log_id.as_u64().to_string(),
                seq_num: SeqNum::default().as_u64().to_string(),
                backlink: None,
                skiplink: None,
            }),
        }
    }
}

/// Determine skiplink entry hash for entry in this log, return `None` when no skiplink is
/// required for the next entry.
pub async fn determine_skiplink(pool: Pool, entry: &Entry) -> Result<Option<Hash>> {
    let next_seq_num = entry.seq_num.clone().next().unwrap();

    // Unwrap as we know that an skiplink exists as soon as previous entry is given
    let skiplink_seq_num = next_seq_num.skiplink_seq_num().unwrap();

    // Check if skiplink is required and return hash if so
    let entry_skiplink_hash = if is_skiplink_required(next_seq_num.as_u64()) {
        let skiplink_entry =
            Entry::at_seq_num(&pool, &entry.author, &entry.log_id, &skiplink_seq_num)
                .await?
                .unwrap();
        Some(skiplink_entry.entry_hash)
    } else {
        None
    };

    Ok(entry_skiplink_hash)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::Configuration;
    use crate::context::Context;
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
                        backlink,
                        logId,
                        seqNum,
                        skiplink
                    }
                }"#,
            }))
            .send()
            .await;

        assert_eq!(
            response.text().await,
            json!({
                "data": {
                    "nextEntryArgs": {
                        "logId": "1",
                        "seqNum": "1",
                        "backlink": null,
                        "skiplink": null
                    }
                }
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn next_entry_args_invalid_author() {
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

        let response_text = response.text().await;
        assert!(response_text.contains("invalid hex encoding in author string"));
    }
}
