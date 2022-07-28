// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use p2panda_rs::document::{DocumentId, DocumentViewId};

/// Input of every task worker containing all information we need to process.
///
/// The workers are designed such that they EITHER await a `DocumentId` OR a `DocumentViewId`.
/// Setting both values `None` or both values `Some` will be rejected.
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct TaskInput {
    /// Specifying a `DocumentId`, indicating that we're interested in processing the "latest"
    /// state of that document.
    pub document_id: Option<DocumentId>,

    /// Specifying a `DocumentViewId`, indicating that we're interested in processing the state of
    /// that document view at this point.
    pub document_view_id: Option<DocumentViewId>,
}

impl TaskInput {
    /// Returns a new instance of `TaskInput`.
    pub fn new(document_id: Option<DocumentId>, document_view_id: Option<DocumentViewId>) -> Self {
        Self {
            document_id,
            document_view_id,
        }
    }
}

impl Display for TaskInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let document_id = match &self.document_id {
            Some(id) => format!("{}", id),
            None => "-".to_string(),
        };

        let view_id = match &self.document_view_id {
            Some(view_id) => format!("{}", view_id),
            None => "-".to_string(),
        };

        write!(f, "<TaskInput {}/{}>", document_id, view_id)
    }
}
