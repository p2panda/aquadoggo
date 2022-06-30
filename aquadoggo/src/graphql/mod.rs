// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod client;
pub mod replication;
pub mod response;
pub mod scalars;
mod schema;

pub use schema::{build_root_schema, MutationRoot, QueryRoot, RootSchema};
