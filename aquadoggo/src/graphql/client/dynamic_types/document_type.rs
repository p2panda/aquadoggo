// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use p2panda_rs::schema::Schema;

use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::client::dynamic_types::DocumentFieldsType;

use super::document_meta_type::DocumentMetaType;

/// Represents a p2panda schema, which appears in GraphQL as a type that can be queried for
/// documents.
pub struct DocumentType(&'static Schema);

impl DocumentType {
    pub fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    /// Access the inner schema.
    pub fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Access the schema's name.
    pub fn type_name(&self) -> String {
        self.schema().id().as_str()
    }

    pub fn register_type(&self, registry: &mut async_graphql::registry::Registry) {
        // Register the type of this schema's `fields` type.
        let fields_type = DocumentFieldsType::new(self.schema());
        fields_type.register_type(registry);

        // Assemble field definitions for this schema itself.
        let mut fields = IndexMap::new();

        // Insert field `meta`.
        fields.insert(
            "meta".to_string(),
            metafield("meta", None, DocumentMetaType::type_name()),
        );

        // Insert field `fields`.
        fields.insert(
            "fields".to_string(),
            metafield("fields", None, &fields_type.type_name()),
        );

        // Finally register the metatype for this schema.
        let metatype = metaobject(&self.type_name(), Some(self.schema().description()), fields);
        registry.types.insert(self.type_name(), metatype);
    }
}
