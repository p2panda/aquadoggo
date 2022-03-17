// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};

use crate::db::Pool;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    // @TODO: Remove this example.
    async fn ping(&self) -> String {
        String::from_str("pong").unwrap()
    }
}

pub type StaticSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_static_schema(pool: Pool) -> StaticSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}
