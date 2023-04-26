// SPDX-License-Identifier: AGPL-3.0-or-later

use dynamic_graphql::SimpleObject;

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar, PublicKeyScalar};

/// Meta fields of a document, contains id and authorship information.
#[derive(SimpleObject)]
pub struct DocumentMeta {
    /// The document id of this document.
    #[graphql(name = "documentId")]
    pub document_id: DocumentIdScalar,

    /// The document view id of this document.
    #[graphql(name = "viewId")]
    pub document_view_id: DocumentViewIdScalar,

    /// The public key of the author who first created this document.
    pub owner: PublicKeyScalar,
}
