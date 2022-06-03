// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{EmptySubscription, Schema};

use crate::db::provider::SqlStorage;
use crate::graphql::client::{Mutation, QueryRoot as ClientQueryRoot};

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<ClientQueryRoot, Mutation, EmptySubscription>;

pub fn build_root_schema(store: SqlStorage) -> RootSchema {
    let query: ClientQueryRoot = ClientQueryRoot::new(store.clone());
    let mutation: Mutation = Mutation::default();

    let s = Schema::build(query, mutation, EmptySubscription)
        .data(store)
        .finish();

    return s;
}
