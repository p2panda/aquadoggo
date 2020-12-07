use anyhow::{Result, Error};
use sqlx::any::{Any, AnyPool, AnyPoolOptions};
use sqlx::migrate;
use sqlx::migrate::MigrateDatabase;

pub type Pool = AnyPool;

/// Create database when not existing
pub async fn create_database(uri: String) -> Result<()> {
    if !Any::database_exists(&uri).await? {
        Any::create_database(&uri).await?;
    }

    Ok(())
}

pub async fn connection_pool(
    uri: String,
    max_connections: u32,
) -> std::result::Result<Pool, Error> {
    // Create connection pool
    let pool: Pool = AnyPoolOptions::new()
        .max_connections(max_connections)
        .connect(&uri)
        .await?;

    Ok(pool)
}

/// Run any pending migrations
pub async fn run_pending_migrations(pool: &Pool) -> Result<()> {
    migrate!("./migrations").run(pool).await?;
    Ok(())
}
