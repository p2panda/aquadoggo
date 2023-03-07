// SPDX-License-Identifier: AGPL-3.0-or-later

//! Provides GraphQL APIs for both replication and interfacing with p2panda clients.
pub mod client;
pub mod pagination;
pub mod replication;
pub mod scalars;
mod schema;

pub use schema::{build_root_schema, GraphQLSchemaManager};
