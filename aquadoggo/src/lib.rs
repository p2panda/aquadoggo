// SPDX-License-Identifier: AGPL-3.0-or-later

//! # aquadoggo
//!
//! Configurable node server implementation for the [`p2panda`] network.
//!
//! [`p2panda`]: https://p2panda.org
//!
//! ## Features
//!
//! - Awaits signed operations from clients via GraphQL.
//! - Verifies the consistency, format and signature of operations and rejects invalid ones.
//! - Stores operations of the network in an SQL database of your choice (SQLite, PostgreSQL).
//! - Materializes views on top of the known data.
//! - Answers filterable and paginated data queries via GraphQL.
//! - Discovers other nodes in local network and internet.
//! - Replicates data with other nodes.
//!
//! ## Example
//!
//! Embed the node server in your Rust application or web container like [`Tauri`]:
//!
//! ```rust,no_run
//! # #[tokio::main]
//! # async fn main() {
//! use aquadoggo::{Configuration, Node};
//!
//! let config = Configuration::default();
//! let node = Node::start(config).await;
//! # }
//! ```
//!
//! [`Tauri`]: https://tauri.studio
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]
#![allow(clippy::uninlined_format_args)]
mod bus;
mod config;
mod context;
mod db;
mod graphql;
mod http;
mod manager;
mod materializer;
mod network;
mod node;
#[cfg(all(test, feature = "proptests"))]
mod proptests;
mod schema;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

pub use crate::config::Configuration;
pub use crate::network::NetworkConfiguration;
pub use node::Node;

/// Init env_logger before the test suite runs to handle logging outputs.
///
/// We output log information using the `log` crate. In itself this doesn't print
/// out any logging information, library users can capture and handle the emitted logs
/// using a log handler. Here we use `env_logger` to handle logs emitted
/// while running our tests.
///
/// This will also capture and output any logs emitted from our dependencies. This behaviour
/// can be customised at runtime. With eg. `RUST_LOG=aquadoggo=info cargo t -- --nocapture` or
/// `RUST_LOG=sqlx=debug cargo t -- --nocapture`.
///
/// The `ctor` crate is used to define a global constructor function. This method will be run
/// before any of the test suites.
#[cfg(test)]
#[ctor::ctor]
fn init() {
    // If the `RUST_LOG` env var is not set skip initiation as we don't want
    // to see any logs.
    if std::env::var("RUST_LOG").is_ok() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
}
