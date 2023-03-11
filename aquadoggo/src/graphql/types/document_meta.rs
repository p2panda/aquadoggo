// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::SimpleObject;

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// The meta fields of a document.
#[derive(SimpleObject)]
pub struct DocumentMeta {
    #[graphql(name = "documentId")]
    pub document_id: DocumentIdScalar,

    #[graphql(name = "viewId")]
    pub view_id: DocumentViewIdScalar,
}
