// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod types;
mod schema_builders;
pub mod mutations;
mod schema;
pub mod scalars;
pub mod utils;

pub use schema::{build_root_schema, GraphQLSchemaManager};
