// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::Pool;
use crate::graphql::Context as GraphQLContext;
use crate::graphql::{build_root_schema, RootSchema};

/// Inner data shared across all services.
pub struct Data {
    // Node configuration.
    pub config: Configuration,

    /// Database connection pool.
    pub pool: Pool,

    /// Root GraphQL schema.
    pub schema: RootSchema,
}

impl Data {
    /// Initialize new data instance with shared database connection pool.
    pub fn new(pool: Pool, config: Configuration) -> Self {
        let graphql_context = GraphQLContext::new(pool.clone());
        let schema = build_root_schema(graphql_context);

        Self {
            config,
            pool,
            schema,
        }
    }
}

/// Data shared across all services.
pub struct Context(pub Arc<Data>);

impl Context {
    /// Returns a new instance of `Context`.
    pub fn new(pool: Pool, config: Configuration) -> Self {
        Self(Arc::new(Data::new(pool, config)))
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
