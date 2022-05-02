use graphql_client::GraphQLQuery;
use super::BambooEntry;
use super::EntryHash;

// The paths are relative to the directory where your `Cargo.toml` is located.
// Both json and the GraphQL schema language are supported as sources for the schema
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "query.graphql",
)]
pub struct GetEntryByHash;
