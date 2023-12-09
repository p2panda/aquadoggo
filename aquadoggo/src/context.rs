// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use p2panda_rs::identity::KeyPair;
use p2panda_rs::storage_provider::traits::{DocumentStore, EntryStore, LogStore, OperationStore};

use crate::bus::ServiceSender;
use crate::config::Configuration;
use crate::db::SqlStore;
use crate::schema::SchemaProvider;

/// Inner data shared across all services.
#[derive(Debug)]
pub struct Data<S>
where
    S: EntryStore + OperationStore + LogStore + DocumentStore,
{
    /// Node authentication and identity provider.
    pub key_pair: KeyPair,

    /// Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: S,

    /// Schema provider gives access to system and application schemas.
    pub schema_provider: SchemaProvider,

    /// Service bus sender.
    pub service_bus: ServiceSender,
}

impl<S> Data<S>
where
    S: EntryStore + OperationStore + LogStore + DocumentStore,
{
    pub fn new(
        store: S,
        key_pair: KeyPair,
        config: Configuration,
        schema_provider: SchemaProvider,
        service_bus: ServiceSender,
    ) -> Self {
        Self {
            key_pair,
            config,
            store,
            schema_provider,
            service_bus,
        }
    }
}

/// Data shared across all services.
#[derive(Debug)]
pub struct Context<S: EntryStore + OperationStore + LogStore + DocumentStore = SqlStore>(
    pub Arc<Data<S>>,
);

impl<S> Context<S>
where
    S: EntryStore + OperationStore + LogStore + DocumentStore,
{
    /// Returns a new instance of `Context`.
    pub fn new(
        store: S,
        key_pair: KeyPair,
        config: Configuration,
        schema_provider: SchemaProvider,
        tx: ServiceSender,
    ) -> Self {
        Self(Arc::new(Data::new(
            store,
            key_pair,
            config,
            schema_provider,
            tx,
        )))
    }
}

impl<S> Clone for Context<S>
where
    S: EntryStore + OperationStore + LogStore + DocumentStore,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S> Deref for Context<S>
where
    S: EntryStore + OperationStore + LogStore + DocumentStore,
{
    type Target = Data<S>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
