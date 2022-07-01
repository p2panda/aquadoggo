// SPDX-License-Identifier: AGPL-3.0-or-later

//! GraphQL types for handling client and replication requests.
pub(crate) mod client;
pub(crate) mod replication;
mod schema;

pub use schema::{build_root_schema, MutationRoot, QueryRoot, RootSchema};
