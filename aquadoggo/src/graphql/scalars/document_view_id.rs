// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::fmt::Display;

use async_graphql::{scalar, Error};
use serde::{Deserialize, Serialize};

/// Document view id as a GraphQL scalar.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentViewId(String);

impl From<p2panda_rs::document::DocumentViewId> for DocumentViewId {
    fn from(view_id: p2panda_rs::document::DocumentViewId) -> Self {
        Self(view_id.as_str())
    }
}

impl TryFrom<DocumentViewId> for p2panda_rs::document::DocumentViewId {
    type Error = Error;

    fn try_from(
        view_id: DocumentViewId,
    ) -> Result<p2panda_rs::document::DocumentViewId, Self::Error> {
        view_id.0.parse().map_err(Into::<Error>::into)
    }
}

impl Display for DocumentViewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

scalar!(DocumentViewId);
