// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::ResolverContext;
use async_graphql::Error;
use dynamic_graphql::{FieldValue, SimpleObject};
use p2panda_rs::document::traits::AsDocument;

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar, PublicKeyScalar};
use crate::graphql::utils::downcast_document;

/// Meta fields of a document, contains id and authorship information.
#[derive(SimpleObject)]
pub struct DocumentMeta {
    /// The document id of this document.
    #[graphql(name = "documentId")]
    document_id: DocumentIdScalar,

    /// The document view id of this document.
    #[graphql(name = "viewId")]
    document_view_id: DocumentViewIdScalar,

    /// The public key of the author who first created this document.
    owner: PublicKeyScalar,
}

impl DocumentMeta {
    /// Resolve `DocumentMeta` as a graphql `FieldValue`.
    ///
    /// Requires a `ResolverContext` to be passed into the method.
    pub async fn resolve(ctx: ResolverContext<'_>) -> Result<Option<FieldValue<'_>>, Error> {
        // Parse the bubble up parent value.
        let document = downcast_document(&ctx);

        // Extract the document in the case of a single or paginated request.
        let document = match document {
            super::DocumentValue::Single(document) => document,
            super::DocumentValue::Paginated(_, _, document) => document,
        };

        // Construct `DocumentMeta` and return it. We defined the document meta
        // type and already registered it in the schema.
        let document_meta = Self {
            document_id: document.id().into(),
            document_view_id: document.view_id().into(),
            owner: document.author().to_owned().into(),
        };

        Ok(Some(FieldValue::owned_any(document_meta)))
    }
}
