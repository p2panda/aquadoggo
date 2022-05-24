// SPDX-License-Identifier: AGPL-3.0-or-later

mod api;
pub mod client;
mod context;
mod replication;
mod schema;

pub use api::{handle_graphql_playground, handle_graphql_query};
pub use client::Query as ClientRoot;
pub use context::Context;
pub use replication::ReplicationRoot;
pub use schema::{build_root_schema, QueryRoot, RootSchema};
