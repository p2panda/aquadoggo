// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod types;
pub mod queries;
pub mod mutations;
mod schema;
pub mod scalars;

pub use schema::{build_root_schema, GraphQLSchemaManager};
