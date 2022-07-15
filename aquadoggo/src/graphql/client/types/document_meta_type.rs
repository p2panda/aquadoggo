// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{Deprecation, MetaEnumValue, MetaType};

use crate::graphql::client::types::utils::{metafield, metaobject};

/// The metatype for generic document metadata.
pub struct DocumentMetaType;

impl DocumentMetaType {
    pub fn name() -> &'static str {
        "DocumentMeta"
    }

    pub fn meta_type() -> MetaType {
        let mut fields = IndexMap::new();

        fields.insert(
            "document_id".to_string(),
            metafield("document_id", None, "String"),
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
