// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::MergedObject;

use crate::graphql::client::{DynamicQuery, StaticQuery};

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject, Debug)]
pub struct ClientRoot(StaticQuery, DynamicQuery);

impl ClientRoot {
    pub fn new() -> Self {
        Self(StaticQuery::default(), DynamicQuery::default())
    }
}
