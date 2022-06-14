// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_graphql::{EmptySubscription, Schema};

use crate::db::provider::SqlStorage;
use crate::graphql::client::query::QueryRoot as ClientQueryRoot;
use crate::graphql::client::Mutation;
use crate::schema_service::{SchemaService, TempFile};

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<ClientQueryRoot, Mutation, EmptySubscription>;

pub async fn build_root_schema(
    store: SqlStorage,
    schema_service: SchemaService<SqlStorage>,
) -> Result<RootSchema> {
    let schemas = schema_service.all_schemas().await?;
    let temp_file = TempFile::save(&schemas, "./aquadoggo-schemas.temp");

    let query: ClientQueryRoot = ClientQueryRoot::new(store.clone(), schema_service.clone());
    let mutation: Mutation = Mutation::default();

    let s = Schema::build(query, mutation, EmptySubscription)
        .data(store)
        .finish();

    temp_file.unlink();
    Ok(s)
}
