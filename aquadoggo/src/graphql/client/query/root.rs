// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::MergedObject;

use crate::db::provider::SqlStorage;
use crate::graphql::client::query::{DynamicQuery, StaticQuery};
use crate::schema_service::SchemaService;

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject, Debug)]
pub struct ClientRoot(StaticQuery, DynamicQuery);

impl ClientRoot {
    pub fn new(store: SqlStorage, schema_service: SchemaService) -> Self {
        Self(
            StaticQuery::default(),
            DynamicQuery::new(store, schema_service),
        )
    }
}
