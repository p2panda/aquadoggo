// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::anyhow;

use graphql_client::{reqwest::post_graphql, GraphQLQuery};
use p2panda_rs::entry::LogId as PandaLogId;
use p2panda_rs::entry::SeqNum as PandaSeqNum;
use p2panda_rs::identity::Author as PandaAuthor;
use reqwest::Client as ReqwestClient;
use reqwest::IntoUrl;

use super::*;

/// A graphql client for doing replication requests to another aquadoggo node.
#[derive(Debug)]
pub struct Client {
    reqwest_client: ReqwestClient,
}

impl Client {
    /// Create a new client.
    pub fn new() -> Self {
        // TODO tls?
        let reqwest_client = ReqwestClient::new();

        Self { reqwest_client }
    }

    /// Attempts to get entries newer than the given sequence_number for an author + log_id.
    ///
    /// Currently does not use pagination, you will need to call this multiple times and eventually
    /// you we will get up to date.
    pub async fn get_entries_newer_than_seq<U: IntoUrl + Clone>(
        &mut self,
        url: U,
        log_id: &PandaLogId,
        author: &PandaAuthor,
        sequence_number: Option<&PandaSeqNum>,
    ) -> Result<Vec<StorageEntry>> {
        let variables =
            create_get_entries_newer_than_seq_request_variable(author, sequence_number, log_id);

        let result =
            post_graphql::<GetEntriesNewerThanSeq, _>(&self.reqwest_client, url.clone(), variables)
                .await?;

        result
            .data
            .and_then(|data| data.get_entries_newer_than_seq.edges)
            .map(convert_edges_to_storage_entries)
            .ok_or_else(|| anyhow!("data wasn't in the format expected"))?
    }
}

fn convert_edges_to_storage_entries(
    edges: Vec<
        Option<get_entries_newer_than_seq::GetEntriesNewerThanSeqGetEntriesNewerThanSeqEdges>,
    >,
) -> Result<Vec<StorageEntry>, Error> {
    // Ooof, the auto generated types aren't very ergonomic to deal with.
    let entries = edges
        .into_iter()
        .flatten()
        .map(|edge| -> Result<StorageEntry> {
            let entry_and_payload = EntryAndPayload {
                entry: edge.node.entry,
                payload: edge.node.payload,
            };
            let storage_entry = entry_and_payload.try_into()?;
            Ok(storage_entry)
        })
        .collect::<Result<Vec<StorageEntry>>>()?;

    Ok(entries)
}

fn create_get_entries_newer_than_seq_request_variable(
    author: &PandaAuthor,
    sequence_number: Option<&PandaSeqNum>,
    log_id: &PandaLogId,
) -> get_entries_newer_than_seq::Variables {
    let author: Author = author.clone().into();
    // We have to do this manual type conversion because of this issue:
    // https://github.com/graphql-rust/graphql-client/issues/386
    let author = get_entries_newer_than_seq::Author {
        publicKey: author.public_key.clone(),
        alias: author.alias.map(|id| id.0),
    };
    let sequence_number =
        sequence_number.map(|sequence_number| SequenceNumber(sequence_number.to_owned()));
    let log_id = LogId(log_id.to_owned());

    get_entries_newer_than_seq::Variables {
        log_id,
        author,
        sequence_number,
        first: None,
        after: None,
    }
}

// The paths are relative to the directory where your `Cargo.toml` is located. Both json and the
// GraphQL schema language are supported as sources for the schema.
#[derive(GraphQLQuery, Clone, Copy, Debug)]
#[graphql(
    schema_path = "src/graphql/replication/client/schema.graphql",
    query_path = "src/graphql/replication/client/queries/get_entry_by_hash.graphql"
)]
struct GetEntryByHash;

// The paths are relative to the directory where your `Cargo.toml` is located. Both JSON and the
// GraphQL schema language are supported as sources for the schema.
#[derive(GraphQLQuery, Debug, Copy, Clone)]
#[graphql(
    schema_path = "src/graphql/replication/client/schema.graphql",
    query_path = "src/graphql/replication/client/queries/get_entries_newer_than_seq.graphql"
)]
struct GetEntriesNewerThanSeq;

// pub async fn get_entries_newer_than_seq()
