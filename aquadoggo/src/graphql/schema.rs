// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::Pool;

use async_graphql::{EmptyMutation, EmptySubscription, Schema, MergedObject};
use super::replication::ReplicationRoot;
use super::ping::PingRoot;

#[derive(MergedObject, Default)]
pub struct QueryRoot(PingRoot, ReplicationRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_root_schema(pool: Pool) -> RootSchema {
    Schema::build(QueryRoot::default(), EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}
