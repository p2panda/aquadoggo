use jsonrpc_core::ErrorCode;
use p2panda_rs::atomic::Hash;
use rand::Rng;
use sqlx::any::Any;
use sqlx::migrate::MigrateDatabase;

use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};

const DB_URL: &str = "sqlite::memory:";

// Create test database
pub async fn initialize_db() -> Pool {
    // Reset database first
    drop_database().await;
    create_database(DB_URL).await.unwrap();

    // Create connection pool and run all migrations
    let pool = connection_pool(DB_URL, 5).await.unwrap();
    run_pending_migrations(&pool).await.unwrap();

    pool
}

// Delete test database
pub async fn drop_database() {
    if Any::database_exists(DB_URL).await.unwrap() {
        Any::drop_database(DB_URL).await.unwrap();
    }
}

// Generate random entry hash
pub fn random_entry_hash() -> String {
    let random_data = rand::thread_rng().gen::<[u8; 32]>().to_vec();

    Hash::new_from_bytes(random_data)
        .unwrap()
        .as_hex()
        .to_owned()
}

// Helper method to generate valid JSON RPC request string
pub fn rpc_request(method: &str, params: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "method": "{}",
            "params": {},
            "id": 1
        }}"#,
        method, params
    )
    .replace(" ", "")
    .replace("\n", "")
}

// Helper method to generate valid JSON RPC response string
pub fn rpc_response(result: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "result": {},
            "id": 1
        }}"#,
        result
    )
    .replace(" ", "")
    .replace("\n", "")
}

// Helper method to generate valid JSON RPC error response string
pub fn rpc_error(code: ErrorCode, message: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "error": {{
                "code": {},
                "message": "<message>"
            }},
            "id": 1
        }}"#,
        code.code(),
    )
    .replace(" ", "")
    .replace("\n", "")
    .replace("<message>", message)
}
