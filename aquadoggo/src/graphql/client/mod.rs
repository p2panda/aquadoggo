// SPDX-License-Identifier: AGPL-3.0-or-later

mod dynamic_query;
pub mod dynamic_types;
mod mutation;
mod query;
mod static_query;
pub mod static_types;

pub use dynamic_query::DynamicQuery;
pub use mutation::ClientMutationRoot;
pub use query::ClientRoot;
pub use static_query::StaticQuery;
