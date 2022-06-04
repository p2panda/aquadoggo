// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct TaskInput {
    pub document_id: Option<DocumentId>,
    pub document_view_id: Option<DocumentViewId>,
}

impl TaskInput {
    pub fn new(document_id: Option<DocumentId>, document_view_id: Option<DocumentViewId>) -> Self {
        Self {
            document_id,
            document_view_id,
        }
    }
}
