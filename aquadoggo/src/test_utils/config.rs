// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Debug;

use once_cell::sync::Lazy;
use serde::Deserialize;

/// Configuration used in test helper methods.
#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct TestConfiguration {
    /// Database url (sqlite or postgres)
    pub database_url: String,
}

impl TestConfiguration {
    /// Create a new configuration object for test environments.
    pub fn new() -> Self {
        envy::from_env::<TestConfiguration>()
            .expect("Could not read environment variables for test configuration")
    }
}

impl Default for TestConfiguration {
    fn default() -> Self {
        Self {
            /// SQLite database stored in memory.
            database_url: "sqlite::memory:".into(),
        }
    }
}

pub static TEST_CONFIG: Lazy<TestConfiguration> = Lazy::new(TestConfiguration::new);
