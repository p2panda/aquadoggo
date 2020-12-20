use anyhow::{Error, Result};
use sqlx::any::{Any, AnyPool, AnyPoolOptions};
use sqlx::migrate;
use sqlx::migrate::MigrateDatabase;

/// Re-export of generic connection pool type.
pub type Pool = AnyPool;

/// Create database when not existing.
pub async fn create_database(url: String) -> Result<()> {
    if !Any::database_exists(&url).await? {
        Any::create_database(&url).await?;
    }

    Ok(())
}

/// Create a database agnostic connection pool.
pub async fn connection_pool(
    url: String,
    max_connections: u32,
) -> std::result::Result<Pool, Error> {
    let pool: Pool = AnyPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await?;

    Ok(pool)
}

/// Run any pending database migrations from inside the application.
pub async fn run_pending_migrations(pool: &Pool) -> Result<()> {
    migrate!("./src/db/migrations").run(pool).await?;
    Ok(())
}
