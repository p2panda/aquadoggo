// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};

/// Represents selection of a document, either by document id or by document view id.
pub enum DocumentSelector {
    /// The document was selected by document id.
    ById(DocumentId),

    /// The document was selected by document view id.
    ByViewId(DocumentViewId),
}
