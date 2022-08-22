// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::config::Configuration;
use crate::context::Context;
use crate::db::provider::SqlStorage;
use crate::schema::SchemaProvider;

/// Container for `SqlStore` with access to the document ids and key_pairs used in the
/// pre-populated database for testing.
pub struct TestDatabase<S: StorageProvider = SqlStorage> {
    pub context: Context<S>,
    pub store: S,
    pub test_data: TestData,
}

impl<S: StorageProvider + Clone> TestDatabase<S> {
    pub fn new(store: S) -> Self {
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
