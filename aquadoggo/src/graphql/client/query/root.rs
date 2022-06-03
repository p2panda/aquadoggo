// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::MergedObject;

use super::{DynamicQuery, StaticQuery};

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject)]
pub struct QueryRoot(StaticQuery, DynamicQuery);

impl QueryRoot {
    pub fn new(pool: crate::db::Pool) -> Self {
        QueryRoot(StaticQuery::default(), DynamicQuery::new(pool))
    }
}
