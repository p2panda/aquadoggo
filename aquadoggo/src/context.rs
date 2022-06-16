// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::provider::SqlStorage;
use crate::schema::SchemaProvider;

/// Inner data shared across all services.
#[derive(Debug)]
pub struct Data {
    /// Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: SqlStorage,

    /// Schema provider gives access to system and application schemas.
    pub schemas: SchemaProvider<SqlStorage>,
}

impl Data {
    pub fn new(
        store: SqlStorage,
        config: Configuration,
        schemas: SchemaProvider<SqlStorage>,
    ) -> Self {
        Self {
            config,
            store,
            schemas,
        }
    }
}

/// Data shared across all services.
#[derive(Debug)]
pub struct Context(pub Arc<Data>);

impl Context {
    /// Returns a new instance of `Context`.
    pub fn new(
        store: SqlStorage,
        config: Configuration,
        schemas: SchemaProvider<SqlStorage>,
    ) -> Self {
        Self(Arc::new(Data::new(store, config, schemas)))
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
        self.0.as_ref()
    }
}
