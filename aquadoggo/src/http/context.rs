// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::graphql::GraphQLSchemaManager;

#[derive(Clone)]
pub struct HttpServiceContext {
    /// Dynamic GraphQL schema manager.
    pub schema: GraphQLSchemaManager,
}

impl HttpServiceContext {
    /// Create a new HttpServiceContext.
    pub fn new(schema: GraphQLSchemaManager) -> Self {
        Self { schema }
    }
}
