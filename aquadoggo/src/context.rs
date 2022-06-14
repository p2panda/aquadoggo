// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::provider::SqlStorage;
use crate::graphql::{build_root_schema, RootSchema};
use crate::schema_service::SchemaService;

/// Inner data shared across all services.
pub struct Data {
    // Node configuration.
    pub config: Configuration,

    /// Storage provider with database connection pool.
    pub store: SqlStorage,

    /// Schema service provides access to all schemas registered on this node.
    pub schema_service: SchemaService<SqlStorage>,

    /// Static GraphQL schema.
    pub graphql_schema: RootSchema,
}

impl Data {
    /// Initialize new data instance with shared database connection pool.
    pub async fn new(store: SqlStorage, config: Configuration) -> Self {
        let schema_service = SchemaService::new(store.clone());
        let graphql_schema = build_root_schema(store.clone(), schema_service.clone())
            .await
            .unwrap();

        Self {
            config,
            store,
            schema_service,
            graphql_schema,
        }
    }
}

/// Data shared across all services.
pub struct Context(pub Arc<Data>);

impl Context {
    /// Returns a new instance of `Context`.
    pub async fn new(store: SqlStorage, config: Configuration) -> Self {
        Self(Arc::new(Data::new(store, config).await))
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
