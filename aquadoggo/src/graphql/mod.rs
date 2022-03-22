// SPDX-License-Identifier: AGPL-3.0-or-later

mod api;
mod schema;

pub use api::{handle_graphql_playground, handle_graphql_query};
pub use schema::{build_static_schema, StaticSchema};
