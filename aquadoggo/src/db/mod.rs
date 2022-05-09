// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{Error, Result};
use sqlx::any::{Any, AnyPool, AnyPoolOptions};
use sqlx::migrate;
use sqlx::migrate::MigrateDatabase;

pub mod errors;
pub mod models;
pub mod provider;
pub mod stores;
pub mod traits;

/// Re-export of generic connection pool type.
pub type Pool = AnyPool;

/// Create database when not existing.
pub async fn create_database(url: &str) -> Result<()> {
    if !Any::database_exists(url).await? {
        Any::create_database(url).await?;
    }

    Any::drop_database(url);

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
