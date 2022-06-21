// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::bus::ServiceSender;
use crate::db::provider::SqlStorage;
use crate::graphql::GraphQLSchemaManager;
use crate::schema_service::SchemaService;

#[derive(Clone)]
pub struct HttpServiceContext {
    /// Dynamic GraphQL schema manager.
    pub schema: GraphQLSchemaManager,
}

impl HttpServiceContext {
    /// Create a new HttpServiceContext.
    pub fn new(store: SqlStorage, tx: ServiceSender, schema_service: SchemaService) -> Self {
        Self {
            schema: GraphQLSchemaManager::new(store, tx, schema_service),
        }
    }
}
