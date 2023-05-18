// SPDX-License-Identifier: AGPL-3.0-or-later

#[cfg(feature = "graphql")]
mod client;
mod config;
mod db;
mod helpers;
mod node;
mod runner;

#[cfg(feature = "graphql")]
pub use client::graphql_test_client;
#[cfg(feature = "graphql")]
pub use client::TestClient;
pub use config::TestConfiguration;
pub use db::{drop_database, initialize_db};
pub use helpers::{build_document, doggo_fields, doggo_schema, schema_from_fields};
pub use node::{
    add_document, add_schema, populate_and_materialize,
    populate_store_config, TestNode,
};
#[cfg(feature="graphql")]
pub use node::add_schema_and_documents;
pub use runner::test_runner;
