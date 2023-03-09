// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod types;
pub mod queries;
mod schema;

pub use schema::{build_root_schema, GraphQLSchemaManager};
