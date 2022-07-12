// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentViewId(p2panda_rs::document::DocumentViewId);

impl From<p2panda_rs::document::DocumentViewId> for DocumentViewId {
    fn from(document_view_id: p2panda_rs::document::DocumentViewId) -> Self {
        Self(document_view_id)
    }
}

impl From<DocumentViewId> for p2panda_rs::document::DocumentViewId {
    fn from(document_view_id: DocumentViewId) -> p2panda_rs::document::DocumentViewId {
        document_view_id.0
    }
}

impl Display for DocumentViewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

scalar!(DocumentViewId);
