// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::provider::SqlStorage;
use crate::schema_service::SchemaService;
use async_graphql::MergedObject;

use super::{DynamicQuery, StaticQuery};

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject)]
pub struct QueryRoot(StaticQuery, DynamicQuery);

impl QueryRoot {
    pub fn new(store: SqlStorage, schema_service: SchemaService<SqlStorage>) -> Self {
        QueryRoot(
            StaticQuery::default(),
            DynamicQuery::new(store, schema_service),
        )
    }
}
