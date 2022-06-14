// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::provider::SqlStorage;
use crate::graphql::{build_root_schema, RootSchema};
use crate::bus::ServiceSender;

#[derive(Clone)]
pub struct HttpServiceContext {
    /// Root GraphQL schema.
    pub schema: RootSchema,
}

impl HttpServiceContext {
    /// Create a new HttpServiceContext.
    pub fn new(store: SqlStorage, tx: ServiceSender) -> Self {
        Self {
            schema: build_root_schema(store, tx),
        }
    }
}
