// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use crate::db::SqlStore;
use crate::graphql::GraphQLSchemaManager;

#[derive(Clone)]
pub struct HttpServiceContext {
    /// SQL database.
    pub store: SqlStore,

    /// Dynamic GraphQL schema manager.
    pub schema: GraphQLSchemaManager,

    /// Path of the directory where blobs should be served from
    pub blob_dir_path: PathBuf,
}

impl HttpServiceContext {
    pub fn new(store: SqlStore, schema: GraphQLSchemaManager, blob_dir_path: PathBuf) -> Self {
        Self {
            store,
            schema,
            blob_dir_path,
        }
    }
}
