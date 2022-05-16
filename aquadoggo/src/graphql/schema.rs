// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{EmptyMutation, EmptySubscription, MergedObject, Schema};

use crate::db::Pool;
use crate::graphql::ClientRoot;

#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub ClientRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_root_schema(pool: Pool) -> RootSchema {
    let client_root: ClientRoot = Default::default();
    let query_root = QueryRoot(client_root);
    Schema::build(query_root, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}
