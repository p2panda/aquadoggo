// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{EmptySubscription, Schema};

use crate::db::provider::SqlStorage;
use crate::graphql::client::{Mutation, Query};

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<Query, Mutation, EmptySubscription>;

pub fn build_root_schema(store: SqlStorage) -> RootSchema {
    let query = Query::default();
    let mutation = Mutation::default();

    Schema::build(query, mutation, EmptySubscription)
        .data(store)
        .finish()
}
