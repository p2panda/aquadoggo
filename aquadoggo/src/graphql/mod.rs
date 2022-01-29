// SPDX-License-Identifier: AGPL-3.0-or-later

mod api;
mod query;
mod sql;

pub use api::{Endpoint, StaticSchema};
pub use query::QueryRoot;
pub use sql::gql_to_sql;
