// SPDX-License-Identifier: AGPL-3.0-or-later

mod dynamic_query;
pub mod dynamic_types;
mod mutation;
mod query;
mod static_query;
mod static_types;
#[cfg(test)]
mod tests;
mod utils;

pub use dynamic_query::DynamicQuery;
pub use mutation::ClientMutationRoot;
pub use query::ClientRoot;
pub use static_query::StaticQuery;
pub use static_types::NextEntryArguments;
