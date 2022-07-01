// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Mutex;

use once_cell::sync::Lazy;
use p2panda_rs::schema::Schema;

static SCHEMA_PROVIDER: Lazy<Mutex<Vec<Schema>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub fn save_static_schemas(data: &[Schema]) {
    let mut schemas = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider");

    schemas.clear();
    schemas.append(&mut data.to_vec());
}

pub fn load_static_schemas() -> &'static Vec<Schema> {
    let data = SCHEMA_PROVIDER
        .lock()
        .expect("Could not acquire mutex lock for static schema provider")
        .to_vec();

    Box::leak(Box::new(data))
}
