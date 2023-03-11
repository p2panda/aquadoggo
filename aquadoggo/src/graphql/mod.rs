// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod mutations;
pub mod scalars;
mod schema;
mod schema_builders;
pub mod types;
pub mod utils;

pub use schema::{build_root_schema, GraphQLSchemaManager};
