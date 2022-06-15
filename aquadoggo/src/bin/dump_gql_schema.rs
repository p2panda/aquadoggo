// SPDX-License-Identifier: AGPL-3.0-or-later

use aquadoggo::graphql::build_root_schema;
use aquadoggo::{connection_pool, SqlStorage};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel(16);
    let pool = connection_pool("sqlite::memory:", 1).await.unwrap();
    let store = SqlStorage::new(pool);
    let schema = build_root_schema(store, tx);
    let sdl = schema.sdl();

    println!("{sdl}");
}
