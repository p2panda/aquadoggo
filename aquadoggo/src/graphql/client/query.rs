// SPDX-License-Identifier: AGPL-3.0-or-later

//! Client API root.
use async_graphql::MergedObject;

use crate::graphql::client::StaticQuery;

/// Root query object for client api that contains a static and a dynamic part.
#[derive(MergedObject, Debug)]
pub struct ClientRoot(StaticQuery);

impl ClientRoot {
    pub fn new() -> Self {
        Self(StaticQuery::default())
    }
}
