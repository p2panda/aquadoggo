// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentId(p2panda_rs::document::DocumentId);

impl From<p2panda_rs::document::DocumentId> for DocumentId {
    fn from(document_id: p2panda_rs::document::DocumentId) -> Self {
        Self(document_id)
    }
}

impl From<DocumentId> for p2panda_rs::document::DocumentId {
    fn from(document_id: DocumentId) -> p2panda_rs::document::DocumentId {
        document_id.0
    }
}

impl Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

scalar!(DocumentId);
