// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::KeyPair;

use crate::config::Configuration;
use crate::context::Context;
use crate::db::SqlStore;
use crate::schema::SchemaProvider;

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase {
    pub context: Context<SqlStore>,
    pub store: SqlStore,
    pub test_data: TestData,
}

impl TestDatabase {
    pub fn new(store: SqlStore) -> Self {
        // Initialise context for store.
        let context = Context::new(
            store.clone(),
            Configuration::default(),
            SchemaProvider::default(),
        );

        // Initialise finished test database.
        TestDatabase {
            context,
            store,
            test_data: TestData::default(),
        }
    }
}

/// Data collected when populating a `TestDatabase` in order to easily check values which
/// would be otherwise hard or impossible to get through the store methods.
#[derive(Default)]
pub struct TestData {
    pub key_pairs: Vec<KeyPair>,
    pub documents: Vec<DocumentId>,
}
