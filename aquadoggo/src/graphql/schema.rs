// SPDX-License-Identifier: AGPL-3.0-or-later

use super::client::{Mutation as ClientMutationRoot, Query as ClientQueryRoot};
use super::ping::PingRoot;
use super::replication::ReplicationRoot;
use super::Context;
use crate::db::provider::SqlStorage;
use async_graphql::{EmptySubscription, MergedObject, Schema};

/// All of the graphql query sub modules merged into one top level root
#[derive(MergedObject, Debug)]
pub struct QueryRoot(
    pub PingRoot,
    pub ReplicationRoot<SqlStorage>,
    pub ClientQueryRoot,
);

/// All of the graphql mutation sub modules merged into one top level root
#[derive(MergedObject, Debug, Copy, Clone)]
pub struct MutationRoot(pub ClientMutationRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Build the root graphql schema that can handle graphql requests.
pub fn build_root_schema(context: Context) -> RootSchema {
    let ping_root: PingRoot = Default::default();
    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let client_query_root = ClientQueryRoot::default();
    let query_root = QueryRoot(ping_root, replication_root, client_query_root);

    let client_mutation_root = Default::default();
    let mutation_root = MutationRoot(client_mutation_root);
    Schema::build(query_root, mutation_root, EmptySubscription)
        .data(context.replication_context)
        .data(context.pool)
        // Add more contexts here if you need, eg:
        //.data(context.ping_context)
        .finish()
}
