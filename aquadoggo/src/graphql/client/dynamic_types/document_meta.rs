// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::{
    Context, Name, OutputType, ScalarType, SelectionField, ServerError, ServerResult, Value,
};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::db::provider::SqlStorage;
use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::client::static_types::{AuthoredOperation, AuthoredOperationList};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar, OperationIdScalar};

/// Name of the field for accessing the document's id.
pub const DOCUMENT_ID_FIELD: &str = "documentId";

/// Name of the field for accessing the document's view id.
pub const VIEW_ID_FIELD: &str = "viewId";

/// Name of the field for accessing the document's operations.
pub const OPERATIONS_FIELD: &str = "operations";

/// The GraphQL type for generic document metadata.
pub struct DocumentMeta;

impl DocumentMeta {
    pub fn type_name() -> &'static str {
        "DocumentMeta"
    }

    /// Generate an object type for generic metadata and register it in a GraphQL schema registry.
    pub fn register_type(registry: &mut async_graphql::registry::Registry) {
        let mut fields = IndexMap::new();

        fields.insert(
            DOCUMENT_ID_FIELD.to_string(),
            metafield(
                DOCUMENT_ID_FIELD,
                Some("The document id of this response object."),
                &*DocumentIdScalar::type_name(),
            ),
        );

        // Manually register scalar type in registry because it's not used in the static api.
        DocumentViewIdScalar::create_type_info(registry);
        AuthoredOperation::create_type_info(registry);

        fields.insert(
            VIEW_ID_FIELD.to_string(),
            metafield(
                VIEW_ID_FIELD,
                Some("The specific document view id contained in this response object."),
                &*DocumentViewIdScalar::type_name(),
            ),
        );

        fields.insert(
            OPERATIONS_FIELD.to_string(),
            metafield(
                OPERATIONS_FIELD,
                Some("An operation contained in this document."),
                &*AuthoredOperation::type_name(),
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
    pub async fn resolve(
        ctx: &Context<'_>,
        root_field: SelectionField<'_>,
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

            if meta_field.name() == OPERATIONS_FIELD && document_id.is_some() {
                let store = ctx.data_unchecked::<SqlStorage>();
                let operations = store
                    .get_operations_by_document_id(document_id.unwrap())
                    .await
                    .expect("Get operations for requested document")
                    .into_iter()
                    .map(|op| {
                        let authored_op: AuthoredOperation = op.into();
                        let mut index_map = IndexMap::new();
                        index_map.insert(
                            Name::new("operationId"),
                            authored_op.operation_id.to_value(),
                        );
                        index_map.insert(Name::new("publicKey"), authored_op.public_key.to_value());
                        if let Some(previous) = authored_op.previous {
                            index_map.insert(Name::new("previous"), previous.to_value());
                        }
                        Value::Object(index_map)
                    })
                    .collect();

                meta_fields.insert(Name::new(OPERATIONS_FIELD), Value::List(operations));
            }
        }
        Ok(Value::Object(meta_fields))
    }
}
