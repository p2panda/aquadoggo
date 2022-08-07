// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::config::Configuration;
use crate::db::provider::SqlStorage;
use crate::schema::SchemaProvider;

/// Inner data shared across all services.
#[derive(Debug)]
pub struct Data<S: StorageProvider> {
    /// Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: S,

    /// Schema provider gives access to system and application schemas.
    pub schema_provider: SchemaProvider,
}

impl<S: StorageProvider> Data<S> {
    pub fn new(store: S, config: Configuration, schema_provider: SchemaProvider) -> Self {
        Self {
            config,
            store,
            schema_provider,
        }
    }
}

/// Data shared across all services.
#[derive(Debug)]
pub struct Context<S: StorageProvider = SqlStorage>(pub Arc<Data<S>>);

impl<S: StorageProvider> Context<S> {
    /// Returns a new instance of `Context`.
    pub fn new(store: S, config: Configuration, schema_provider: SchemaProvider) -> Self {
        Self(Arc::new(Data::new(store, config, schema_provider)))
    }
}

impl<S: StorageProvider> Clone for Context<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: StorageProvider> Deref for Context<S> {
    type Target = Data<S>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
