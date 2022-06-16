// SPDX-License-Identifier: AGPL-3.0-or-later

//! # aquadoggo
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

mod bus;
mod config;
mod context;
mod db;
mod errors;
/// Aquadoggo graphql types for handling client and replication requests
pub mod graphql;
mod http;
mod manager;
mod materializer;
mod node;
mod schema;

#[cfg(test)]
mod test_helpers;

pub use config::Configuration;
pub use db::{connection_pool, provider::SqlStorage};
pub use node::Node;
