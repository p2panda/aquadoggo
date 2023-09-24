// SPDX-License-Identifier: AGPL-3.0-or-later

mod client;
mod config;
mod db;
pub mod helpers;
mod node;
mod runner;

pub use client::{http_test_client, TestClient};
pub use config::TestConfiguration;
pub use db::{drop_database, initialize_db, initialize_sqlite_db};
pub use helpers::{
    build_document, doggo_fields, doggo_schema, generate_key_pairs, schema_from_fields,
};
pub use node::{
    add_blob, add_document, add_schema, add_schema_and_documents, assert_query,
    populate_and_materialize_unchecked, populate_store_config, populate_store_unchecked,
    update_blob, update_document, PopulateStoreConfig, TestNode,
};
pub use runner::{test_runner, test_runner_with_manager, TestNodeManager};
