// SPDX-License-Identifier: AGPL-3.0-or-later

//! API for p2panda clients to publish and query data on this node.
mod mutation;
mod query;
mod static_query;
mod static_types;
#[cfg(test)]
mod tests;
mod utils;

pub use mutation::ClientMutationRoot;
pub use query::ClientRoot;
pub use static_query::StaticQuery;
pub use static_types::NextArguments;
