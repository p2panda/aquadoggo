// SPDX-License-Identifier: AGPL-3.0-or-later

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
