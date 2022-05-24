// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::{provider::SqlStorage, Pool};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::replication::context::Context as ReplicationContext;

#[derive(Debug)]
/// The combined graphql contexts used by the various sub modules
pub struct Context {
    /// The replication context
    pub replication_context: Arc<Mutex<ReplicationContext<SqlStorage>>>,
    /// The db connection pool used by the client
    pub pool: Pool,
}

impl Context {
    /// Create a new Context
    pub fn new(pool: Pool) -> Self {
        let storage_provider = SqlStorage { pool: pool.clone() };

        let replication_context =
            Arc::new(Mutex::new(ReplicationContext::new(1000, storage_provider)));

        Self {
            replication_context,
            pool
        }
    }
}
