// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::db::provider::SqlStorage;

use super::replication::context::Context as ReplicationContext;

#[derive(Debug)]
/// The combined graphql contexts used by the various sub modules
pub struct Context {
    /// The replication context
    pub replication_context: Arc<Mutex<ReplicationContext<SqlStorage>>>,
    /// The db connection pool used by the client
    pub store: SqlStorage,
}

impl Context {
    /// Create a new Context
    pub fn new(store: SqlStorage) -> Self {
        let replication_context =
            Arc::new(Mutex::new(ReplicationContext::new(1000, store.clone())));

        Self {
            replication_context,
            store,
        }
    }
}
