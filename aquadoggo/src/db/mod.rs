// SPDX-License-Identifier: AGPL-3.0-or-later

//! Persistent storage for an `aquadoggo` node supporting both Postgres and SQLite databases.
//!
//! The main interface is [`SqlStore`] which offers an interface onto the database by implementing
//! the storage traits defined in `p2panda-rs` as well as some implementation specific features.
use anyhow::{Error, Result};
use sqlx::any::{Any, AnyPool, AnyPoolOptions};
use sqlx::migrate;
use sqlx::migrate::MigrateDatabase;

pub mod errors;
pub mod models;
#[cfg(feature = "graphql")]
pub mod query;
pub mod stores;
pub mod types;

/// SQL based persistent storage that implements `EntryStore`, `OperationStore`, `LogStore` and `DocumentStore`.
#[derive(Clone, Debug)]
pub struct SqlStore {
    pub(crate) pool: Pool,
}

impl SqlStore {
    /// Create a new `SqlStore` using the provided db `Pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// Re-export of generic connection pool type.
pub type Pool = AnyPool;

/// Create database when not existing.
pub async fn create_database(url: &str) -> Result<()> {
    if !Any::database_exists(url).await? {
        Any::create_database(url).await?;
    }

    Ok(())
}

/// Create a database agnostic connection pool.
pub async fn connection_pool(url: &str, max_connections: u32) -> Result<Pool, Error> {
    let pool: Pool = AnyPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;

    Ok(pool)
}

/// Run any pending database migrations from inside the application.
pub async fn run_pending_migrations(pool: &Pool) -> Result<()> {
    migrate!().run(pool).await?;
    Ok(())
}
