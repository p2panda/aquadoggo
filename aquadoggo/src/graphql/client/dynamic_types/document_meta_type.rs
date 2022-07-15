// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::{Name, OutputType, ScalarType, SelectionField, Value};
use p2panda_rs::document::{DocumentId, DocumentViewId};

use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::scalars::DocumentId as DocumentIdScalar;

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
            metafield("documentId", None, &*DocumentIdScalar::type_name()),
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

    /// Resolve GraphQL response value for metadata query field.
    ///
    /// All parameters that are available should be set.
    // Override rule to avoid unnecessary nesting.
    #[allow(clippy::unnecessary_unwrap)]
    pub fn resolve(
        root_field: SelectionField,
        document_id: Option<&DocumentId>,
        view_id: Option<&DocumentViewId>,
    ) -> Value {
        let mut meta_fields = IndexMap::new();
        for meta_field in root_field.selection_set() {
            if meta_field.name() == "documentId" && document_id.is_some() {
                meta_fields.insert(
                    Name::new("documentId"),
                    DocumentIdScalar::from(document_id.unwrap().to_owned()).to_value(),
                );
            }

            if meta_field.name() == "documentViewId" && view_id.is_some() {
                meta_fields.insert(
                    Name::new("documentViewId"),
                    Value::String(view_id.unwrap().as_str().to_string()),
                );
            }
        }
        Value::Object(meta_fields)
    }
}
