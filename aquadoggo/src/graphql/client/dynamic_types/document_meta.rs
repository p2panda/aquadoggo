// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::{
    Name, OutputType, ScalarType, SelectionField, ServerError, ServerResult, Value,
};
use p2panda_rs::document::{DocumentId, DocumentViewId};

use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

/// Name of the field for accessing the document's id.
pub const DOCUMENT_ID_FIELD: &str = "documentId";

/// Name of the field for accessing the document's view id.
pub const VIEW_ID_FIELD: &str = "viewId";

/// The GraphQL type for generic document metadata.
pub struct DocumentMeta;

impl DocumentMeta {
    pub fn type_name() -> &'static str {
        "DocumentMeta"
    }

    /// Generate an object type for generic metadata and register it in a GraphQL schema registry.
    pub fn register_type(registry: &mut async_graphql::registry::Registry) {
        // Important!
        //
        // Manually register scalar type in registry because it's not used in the static api.
        DocumentIdScalar::create_type_info(registry);

        let mut fields = IndexMap::new();

        fields.insert(
            DOCUMENT_ID_FIELD.to_string(),
            metafield(
                DOCUMENT_ID_FIELD,
                Some("The document id of this response object."),
                &DocumentIdScalar::type_name(),
            ),
        );

        fields.insert(
            VIEW_ID_FIELD.to_string(),
            metafield(
                VIEW_ID_FIELD,
                Some("The specific document view id contained in this response object."),
                &DocumentViewIdScalar::type_name(),
            ),
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
    ) -> ServerResult<Value> {
        let mut meta_fields = IndexMap::<Name, Value>::new();

        for meta_field in root_field.selection_set() {
            let response_key = Name::new(meta_field.alias().unwrap_or_else(|| meta_field.name()));

            match meta_field.name() {
                "__typename" => {
                    meta_fields.insert(
                        response_key,
                        Value::String(DocumentMeta::type_name().to_string()),
                    );
                }
                DOCUMENT_ID_FIELD => {
                    if let Some(document_id) = document_id {
                        meta_fields
                            .insert(response_key, DocumentIdScalar::from(document_id).to_value());
                    }
                }
                VIEW_ID_FIELD => {
                    if let Some(view_id) = view_id {
                        meta_fields.insert(response_key, Value::String(view_id.to_string()));
                    }
                }
                _ => Err(ServerError::new(
                    format!(
                        "Field '{}' does not exist on {}",
                        meta_field.name(),
                        Self::type_name(),
                    ),
                    None,
                ))?,
            }
        }
        Ok(Value::Object(meta_fields))
    }
}
