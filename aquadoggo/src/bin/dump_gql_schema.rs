// SPDX-License-Identifier: AGPL-3.0-or-later

use aquadoggo::graphql::build_root_schema;
use aquadoggo::schema_service::SchemaService;
use aquadoggo::{connection_pool, SqlStorage};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel(16);
    let pool = connection_pool("sqlite::memory:", 1).await.unwrap();
    let store = SqlStorage::new(pool);
    let schema_service = SchemaService::new(store.clone());
    let schema = build_root_schema(store, tx, schema_service);
    let sdl = schema.sdl();

    println!("{sdl}");
}
