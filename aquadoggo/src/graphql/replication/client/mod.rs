// SPDX-License-Identifier: AGPL-3.0-or-later

use super::Entry;
use super::EntryHash;
use graphql_client::GraphQLQuery;

// The paths are relative to the directory where your `Cargo.toml` is located.
// Both json and the GraphQL schema language are supported as sources for the schema
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/graphql/replication/client/schema.graphql",
    query_path = "src/graphql/replication/client/queries/get_entry_by_hash.graphql"
)]
pub struct GetEntryByHash;
