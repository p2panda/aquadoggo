// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod client;
mod graphql_schema;
pub mod pagination;
pub mod replication;
pub mod scalars;

pub use graphql_schema::{build_root_schema, GraphQLSchemaManager, QueryRoot, RootSchema};
