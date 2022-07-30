// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::fmt::Display;

use async_graphql::{scalar, Error};
use p2panda_rs::hash::Hash;
use serde::{Deserialize, Serialize};

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentId(String);

impl From<p2panda_rs::document::DocumentId> for DocumentId {
    fn from(document_id: p2panda_rs::document::DocumentId) -> Self {
        Self(document_id.as_str().to_string())
    }
}

impl TryFrom<DocumentId> for p2panda_rs::document::DocumentId {
    type Error = Error;

    fn try_from(document_id: DocumentId) -> Result<p2panda_rs::document::DocumentId, Self::Error> {
        let hash = document_id
            .0
            .parse::<Hash>()
            .map_err(|err| Into::<Error>::into(err))?;
        Ok(p2panda_rs::document::DocumentId::from(hash))
    }
}

impl Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

scalar!(DocumentId);
