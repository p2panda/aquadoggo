// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::provider::SqlStorage;
use crate::graphql::{build_root_schema, RootSchema};

/// Inner data shared across all services.
pub struct Data {
    // Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: SqlStorage,

    /// Static GraphQL schema.
    pub schema: RootSchema,
}

impl Data {
    /// Initialize new data instance with shared database connection pool.
    pub fn new(store: SqlStorage, config: Configuration) -> Self {
        let schema = build_root_schema(store.clone());

        Self {
            config,
            store,
            schema,
        }
    }
}

/// Data shared across all services.
pub struct Context(pub Arc<Data>);

impl Context {
    /// Returns a new instance of `Context`.
    pub fn new(store: SqlStorage, config: Configuration) -> Self {
        Self(Arc::new(Data::new(store, config)))
    }
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Deref for Context {
    type Target = Data;

    fn deref(&self) -> &Self::Target {
        &self.0.as_ref()
    }
}
