// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use p2panda_rs::document::{DocumentId, DocumentViewId};

/// Input of every task worker containing all information we need to process.
///
/// The workers are designed such that they EITHER await a `DocumentId` OR a `DocumentViewId`.
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum TaskInput {
    /// Specifying a `Document`, indicating that we're interested in processing the "latest"
    /// state of that document.
    DocumentId(DocumentId),

    /// Specifying a `SpecificView`, indicating that we're interested in processing the state of
    /// that document view at this point.
    SpecificView(DocumentViewId),

    /// Specifying a `CurrentView`, indicating that we're interested in processing the state of
    /// that document view at this point and that it is the current view.
    CurrentView(DocumentViewId),
}

impl Display for TaskInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let document_id = match &self {
            Self::DocumentId(id) => format!("{}", id),
            Self::SpecificView(_) | Self::CurrentView(_) => "-".to_string(),
        };

        let view_id = match &self {
            Self::SpecificView(view_id) | Self::CurrentView(view_id) => format!("{}", view_id),
            Self::DocumentId(_) => "-".to_string(),
        };

        write!(f, "<TaskInput {}/{}>", document_id, view_id)
    }
}
