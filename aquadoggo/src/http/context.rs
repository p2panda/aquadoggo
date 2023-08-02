// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use crate::graphql::GraphQLSchemaManager;

#[derive(Clone)]
pub struct HttpServiceContext {
    /// Dynamic GraphQL schema manager.
    pub schema: GraphQLSchemaManager,

    /// Path of the directory where blobs should be served from
    pub blob_dir_path: PathBuf,
}

impl HttpServiceContext {
    /// Create a new HttpServiceContext.
    pub fn new(schema: GraphQLSchemaManager, blob_dir_path: PathBuf) -> Self {
        Self {
            schema,
            blob_dir_path,
        }
    }
}
