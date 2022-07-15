// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::MetaType;
use async_graphql::OutputType;

use crate::graphql::client::types::utils::{metafield, metaobject};
use crate::graphql::scalars::{DocumentId, DocumentViewId};

/// The metatype for generic document metadata.
pub struct DocumentMetaType;

// Disable this rule to be able to use `&*` to access `Cow` values.
#[allow(clippy::explicit_auto_deref)]
impl DocumentMetaType {
    pub fn name() -> &'static str {
        "DocumentMeta"
    }

    pub fn meta_type() -> MetaType {
        let mut fields = IndexMap::new();

        fields.insert(
            "document_id".to_string(),
            metafield("document_id", None, &*DocumentId::type_name()),
        );

        fields.insert(
            "document_view_id".to_string(),
            metafield("document_view_id", None, "String"),
        );

        metaobject(
            Self::name(),
            Some("Metadata for documents of this schema."),
            fields,
        )
    }
}
