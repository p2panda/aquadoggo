// SPDX-License-Identifier: AGPL-3.0-or-later

mod api;
pub mod client;
mod schema;

pub use api::{handle_graphql_playground, handle_graphql_query};
pub use schema::{build_root_schema, RootSchema};
