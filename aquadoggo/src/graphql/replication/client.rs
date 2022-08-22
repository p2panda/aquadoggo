// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::{anyhow, Result};
use gql_client::Client;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::Author;
use serde::{Deserialize, Serialize};

use crate::db::stores::StorageEntry;
use crate::graphql::pagination::Paginated;
use crate::graphql::replication::response::EncodedEntryAndOperation;
use crate::graphql::scalars;

/// Response type of `entries_newer_than_seq_num` query.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    entries_newer_than_seq_num: Paginated<EncodedEntryAndOperation, scalars::SeqNumScalar>,
}

/// Attempts to get entries newer than the given sequence number for a public key and log id.
pub async fn entries_newer_than_seq_num(
    endpoint: &str,
    log_id: &LogId,
    public_key: &Author,
    latest_seq_num: Option<&SeqNum>,
) -> Result<Vec<StorageEntry>> {
    // @TODO: Currently this method does not use pagination, you will need to call this multiple
    // times and eventually you we will get up to date.
    let query = format!(
        r#"
            query {{
                entriesNewerThanSeqNum(
                    logId: "{}",
                    publicKey: "{}",
                    seqNum: {}
                ) {{
                    edges {{
                        cursor
                        node {{
                            entry
                            operation
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                    }}
                }}
            }}
        "#,
        log_id.as_u64(),
        public_key.as_str(),
        latest_seq_num
            .map(|num| num.as_u64().to_string())
            .unwrap_or_else(|| "null".into()),
    );

    // Make GraphQL request
    let client = Client::new(endpoint);
    let response: Response = client
        .query_unwrap(&query)
        .await
        .map_err(|err| anyhow!("Replication query failed with error {}", err))?;

    // Convert results to correct return type
    let entries = response
        .entries_newer_than_seq_num
        .edges
        .into_iter()
        .map(|edge| edge.node.try_into())
        .collect::<Result<Vec<StorageEntry>>>()?;

    Ok(entries)
}
