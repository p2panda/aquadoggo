// SPDX-License-Identifier: AGPL-3.0-or-later

use graphql_client::{reqwest::post_graphql, GraphQLQuery};
use p2panda_rs::entry::LogId as PandaLogId;
use p2panda_rs::entry::SeqNum as PandaSeqNum;
use p2panda_rs::identity::Author as PandaAuthor;
use reqwest::Client as ReqwestClient;
use reqwest::IntoUrl;
use reqwest::Url;

use super::*;

pub struct Client {
    reqwest_client: ReqwestClient,
}

impl Client {
    pub fn new() -> Self {
        // TODO tls?
        let reqwest_client = ReqwestClient::new();

        Self { reqwest_client }
    }

    /// Attempts to get all entries newer than the given sequence_number for and author + log_id.
    ///
    /// Manages pagination of graphql internally
    pub async fn get_entries_newer_than_seq<U: IntoUrl + Clone>(
        &mut self,
        url: U,
        log_id: PandaLogId,
        author: PandaAuthor,
        sequence_number: PandaSeqNum,
    ) -> Result<Vec<EntryAndPayload>> {
        let author: Author = author.into();

        // We have to do this manual type conversion because of this issue: https://github.com/graphql-rust/graphql-client/issues/386
        let create_variables = {
            let author = author.clone();

            move |after: Option<String>| -> get_entries_newer_than_seq::Variables {
                let author = get_entries_newer_than_seq::Author {
                    publicKey: author.public_key.clone(),
                    alias: author.alias.clone().map(|id| id.0),
                };
                let sequence_number = SequenceNumber(sequence_number);
                let log_id = LogId(log_id);

                get_entries_newer_than_seq::Variables {
                    log_id,
                    author,
                    sequence_number,
                    first: None,
                    after,
                }
            }
        };
        loop {
            let variables = create_variables(None);
            let result = post_graphql::<GetEntriesNewerThanSeq, _>(
                &self.reqwest_client,
                url.clone(),
                variables,
            )
            .await?;

            match result.data {
                Some(data) => {
                    if !data.get_entries_newer_than_seq.page_info.has_next_page {
                        break;
                    }
                }
                _ => break,
            }
        }

        todo!()
    }
}

// The paths are relative to the directory where your `Cargo.toml` is located.
// Both json and the GraphQL schema language are supported as sources for the schema
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/graphql/replication/client/schema.graphql",
    query_path = "src/graphql/replication/client/queries/get_entry_by_hash.graphql"
)]
pub struct GetEntryByHash;

// The paths are relative to the directory where your `Cargo.toml` is located.
// Both json and the GraphQL schema language are supported as sources for the schema
#[derive(GraphQLQuery, Debug, Copy, Clone)]
#[graphql(
    schema_path = "src/graphql/replication/client/schema.graphql",
    query_path = "src/graphql/replication/client/queries/get_entries_newer_than_seq.graphql"
)]
pub struct GetEntriesNewerThanSeq;

//pub async fn get_entries_newer_than_seq()
