// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Debug;

use serde::Deserialize;

/// Configuration used in test helper methods.
#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct TestConfiguration {
    /// Database url (SQLite or PostgreSQL).
    pub database_url: String,
}

impl TestConfiguration {
    pub fn new() -> Self {
        envy::from_env::<TestConfiguration>()
            .expect("Could not read environment variables for test configuration")
    }
}

impl Default for TestConfiguration {
    fn default() -> Self {
        // Give each database an unique name
        let db_name = format!("dbmem{}", rand::random::<u32>());

        Self {
            /// SQLite database stored in memory.
            database_url: format!("sqlite://{db_name}?mode=memory&cache=private"),
        }
    }
}
