// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::OutputType;

use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::scalars::DocumentId;

/// The metatype for generic document metadata.
pub struct DocumentMetaType;

// Disable this rule to be able to use `&*` to access `Cow` values.
#[allow(clippy::explicit_auto_deref)]
impl DocumentMetaType {
    pub fn type_name() -> &'static str {
        "DocumentMeta"
    }

    pub fn register_type(&self, registry: &mut async_graphql::registry::Registry) {
        let mut fields = IndexMap::new();

        fields.insert(
            "documentId".to_string(),
            metafield("documentId", None, &*DocumentId::type_name()),
        );

        fields.insert(
            "documentViewId".to_string(),
            metafield("documentViewId", None, "String"),
        );

        registry.types.insert(
            Self::type_name().to_string(),
            metaobject(
                Self::type_name(),
                Some("Metadata for documents of this schema."),
                fields,
            ),
        );
    }
}
