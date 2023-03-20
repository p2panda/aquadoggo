// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::ResolverContext;
use async_graphql::Error;
use dynamic_graphql::{FieldValue, SimpleObject};
use p2panda_rs::document::traits::AsDocument;

use crate::db::SqlStore;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::{downcast_document_id_arguments, get_document_from_params};

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
    pub async fn resolve<'a>(ctx: ResolverContext<'a>) -> Result<Option<FieldValue<'a>>, Error> {
        let store = ctx.data_unchecked::<SqlStore>();

        // Downcast the parameters passed up from the parent query field
        let (document_id, document_view_id) = downcast_document_id_arguments(&ctx);

        // Get the whole document
        let document = get_document_from_params(store, &document_id, &document_view_id).await?;

        // Construct `DocumentMeta` and return it. We defined the document meta
        // type and already registered it in the schema. It's derived resolvers
        // will handle field selection.
        let field_value = match document {
            Some(document) => {
                let document_meta = Self {
                    document_id: document.id().into(),
                    view_id: document.view_id().into(),
                };
                Some(FieldValue::owned_any(document_meta))
            }
            None => Some(FieldValue::NULL),
        };

        Ok(field_value)
    }
}
