// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::provider::SqlStorage;
use crate::graphql::Context as GraphQLContext;
use crate::graphql::{build_root_schema, RootSchema};

/// Inner data shared across all services.
pub struct Data {
    /// Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: SqlStorage,

    /// Root GraphQL schema.
    pub schema: RootSchema,
}

impl Debug for Data {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        // Omit the the schema field for now, it doesn't implement debug. Some debug info is better than none.
        fmt.debug_struct("Data")
            .field("config", &self.config)
            .field("store", &self.store)
            .finish()
    }
}

impl Data {
    pub fn new(store: SqlStorage, config: Configuration) -> Self {
        let graphql_context = GraphQLContext::new(store.clone());
        let schema = build_root_schema(graphql_context);

        Self {
            config,
            store,
            schema,
        }
    }
}

/// Data shared across all services.
#[derive(Debug)]
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
        self.0.as_ref()
    }
}
