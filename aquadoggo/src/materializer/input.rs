// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

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

impl Display for TaskInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let doc_id = match &self.document_id {
            Some(id) => format!("{}", id),
            None => "-".to_string(),
        };
        let view_id = match &self.document_view_id {
            Some(view_id) => format!("{}", view_id),
            None => "-".to_string(),
        };
        write!(f, "<TaskInput {}/{}>", doc_id, view_id)
    }
}
