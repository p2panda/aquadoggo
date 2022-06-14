// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{EmptySubscription, Schema};

use crate::db::provider::SqlStorage;
use crate::db::traits::SchemaStore;
use crate::graphql::client::query::{save_temp, unlink_temp, QueryRoot as ClientQueryRoot};
use crate::graphql::client::Mutation;

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<ClientQueryRoot, Mutation, EmptySubscription>;

pub async fn build_root_schema(store: SqlStorage) -> RootSchema {
    let schemas = store.get_all_schema().await.unwrap();
    save_temp(&schemas);

    let query: ClientQueryRoot = ClientQueryRoot::new(store.clone());
    let mutation: Mutation = Mutation::default();

    let s = Schema::build(query, mutation, EmptySubscription)
        .data(store)
        .finish();

    unlink_temp();

    return s;
}
