// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::ResolverContext;
use async_graphql::Error;
use dynamic_graphql::{FieldValue, SimpleObject};
use p2panda_rs::document::traits::AsDocument;

use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::downcast_document;

/// The meta fields of a document.
#[derive(SimpleObject)]
pub struct DocumentMeta {
    #[graphql(name = "documentId")]
    pub document_id: DocumentIdScalar,

    #[graphql(name = "viewId")]
    pub view_id: DocumentViewIdScalar,
}

impl DocumentMeta {
    /// Resolve `DocumentMeta` as a graphql `FieldValue`.
    ///
    /// Requires a `ResolverContext` to be passed into the method.
    pub async fn resolve(ctx: ResolverContext<'_>) -> Result<Option<FieldValue<'_>>, Error> {
        // Parse the bubble up value.
        let document = downcast_document(&ctx);

        let document = match document {
            super::DocumentValue::Single(document) => document,
            super::DocumentValue::Paginated(_, _, document) => document,
        };

        // Construct `DocumentMeta` and return it. We defined the document meta
        // type and already registered it in the schema. It's derived resolvers
        // will handle field selection.
        let document_meta = Self {
            document_id: document.id().into(),
            view_id: document.view_id().into(),
        };

        Ok(Some(FieldValue::owned_any(document_meta)))
    }
}
