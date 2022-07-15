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

        fields.insert(
            "status".to_string(),
            metafield("status", None, "DocumentStatus"),
        );

        metaobject(
            Self::name(),
            Some("Metadata for documents of this schema."),
            fields,
        )
    }

    /// Return metatype for materialisation status of document.
    pub fn status_type() -> MetaType {
        let mut enum_values = IndexMap::new();

        enum_values.insert(
            "Unavailable",
            MetaEnumValue {
                name: "Unavailable",
                description: Some("We don't have any information about this document."),
                deprecation: Deprecation::NoDeprecated,
                visible: None,
            },
        );

        enum_values.insert(
            "Incomplete",
            MetaEnumValue {
                name: "Incomplete",
                description: Some(
                    "We have some operations for this document but it's not materialised yet.",
                ),
                deprecation: Deprecation::NoDeprecated,
                visible: None,
            },
        );

        enum_values.insert(
            "Ok",
            MetaEnumValue {
                name: "Ok",
                description: Some("The document has some materialised view available."),
                deprecation: Deprecation::NoDeprecated,
                visible: None,
            },
        );

        MetaType::Enum {
            name: "DocumentStatus".to_string(),
            description: None,
            enum_values,
            visible: None,
            rust_typename: "__fake__",
        }
    }
}
