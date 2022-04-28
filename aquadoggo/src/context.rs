// SPDX-License-Identifier: AGPL-3.0-or-later

use std::ops::Deref;
use std::sync::Arc;

use crate::config::Configuration;
use crate::db::Pool;
use crate::graphql::{build_static_schema, StaticSchema};
use crate::rpc::{build_rpc_api_service, RpcApiService};

/// Inner data shared across all services.
pub struct Data {
    // Node configuration.
    pub config: Configuration,

    /// Database connection pool.
    pub pool: Pool,

    /// Static GraphQL schema.
    pub schema: StaticSchema,

    /// JSON RPC service with RPC handlers.
    // @TODO: This will be removed soon. See: https://github.com/p2panda/aquadoggo/issues/60
    pub rpc_service: RpcApiService,
}

impl Data {
    /// Initialize new data instance with shared database connection pool.
    pub fn new(pool: Pool, config: Configuration) -> Self {
        let schema = build_static_schema(pool.clone());
        let rpc_service = build_rpc_api_service(pool.clone());

        Self {
            config,
            pool,
            schema,
            rpc_service,
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
