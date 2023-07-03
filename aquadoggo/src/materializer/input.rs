// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use p2panda_rs::document::{DocumentId, DocumentViewId};

/// Input of every task worker containing all information we need to process.
///
/// The workers are designed such that they EITHER await a `DocumentId` OR a `DocumentViewId`.
/// Setting both values `None` or both values `Some` will be rejected.
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum TaskInput {
    /// Specifying a `DocumentId`, indicating that we're interested in processing the "latest"
    /// state of that document.
    DocumentId(DocumentId),

    /// Specifying a `DocumentViewId`, indicating that we're interested in processing the state of
    /// that document view at this point.
    DocumentViewId(DocumentViewId),
}

impl Display for TaskInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let document_id = match &self {
            Self::DocumentId(id) => format!("{}", id),
            Self::DocumentViewId(_) => "-".to_string(),
        };

        let view_id = match &self {
            Self::DocumentViewId(view_id) => format!("{}", view_id),
            Self::DocumentId(_) => "-".to_string(),
        };

        write!(f, "<TaskInput {}/{}>", document_id, view_id)
    }
}
