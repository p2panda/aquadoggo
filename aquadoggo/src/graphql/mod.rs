// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod constants;
pub mod mutations;
pub mod queries;
pub mod scalars;
mod schema;
pub mod types;
pub mod utils;
#[cfg(test)]
mod tests;

pub use schema::{build_root_schema, GraphQLSchemaManager};
