// SPDX-License-Identifier: AGPL-3.0-or-later

use super::ping::PingRoot;
use super::replication::ReplicationRoot;
use super::Context;
use crate::db::provider::SqlStorage;
use async_graphql::{EmptyMutation, EmptySubscription, MergedObject, Schema};

/// All of the graphql sub modules merged into one top level root
#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub PingRoot, pub ReplicationRoot<SqlStorage>);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Build the root graphql schema that can handle graphql requests.
pub fn build_root_schema(context: Context) -> RootSchema {
    let ping_root: PingRoot = Default::default();
    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let query_root = QueryRoot(ping_root, replication_root);
    Schema::build(query_root, EmptyMutation, EmptySubscription)
        .data(context.replication_context)
        // Add more contexts here if you need, eg:
        //.data(context.ping_context)
        .finish()
}
