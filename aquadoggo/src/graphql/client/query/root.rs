// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::MergedObject;

use crate::graphql::client::query::{DynamicQuery, StaticQuery};
use crate::schema::SchemaProvider;

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject, Debug)]
pub struct ClientRoot(StaticQuery, DynamicQuery);

impl ClientRoot {
    pub fn new(schema_provider: SchemaProvider) -> Self {
        Self(StaticQuery::default(), DynamicQuery::new(schema_provider))
    }
}
