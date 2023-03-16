// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use p2panda_rs::schema::Schema;

use crate::graphql::client::dynamic_types::utils::{metafield, metaobject};
use crate::graphql::client::dynamic_types::{DocumentFields, DocumentMeta};

/// Fieldname on document for accessing document metadata.
pub const META_FIELD: &str = "meta";

/// Fieldname on document for accessing document view fields.
pub const FIELDS_FIELD: &str = "fields";

/// Represents documents of a p2panda schema.
pub struct Document(&'static Schema);

impl Document {
    /// Get a new instance for the given schema, which must be `static`.
    pub fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    /// Access the inner schema.
    pub fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Access the schema's name.
    pub fn type_name(&self) -> String {
        self.schema().id().to_string()
    }

    /// Generate an object type that represents documents of this schema in the GraphQL API.
    ///
    /// Be mindful when changing field names as these also have to be changed in the dynamic query
    /// resolver to match.
    pub fn register_type(&self, registry: &mut async_graphql::registry::Registry) {
        // Register the type of this schema's `fields` type.
        let fields_type = DocumentFields::new(self.schema());
        fields_type.register_type(registry);

        // Assemble field definitions for this schema itself.
        let mut fields = IndexMap::new();

        // Insert field `meta`.
        fields.insert(
            META_FIELD.to_string(),
            metafield(META_FIELD, None, DocumentMeta::type_name()),
        );

        // Insert field `fields`.
        fields.insert(
            FIELDS_FIELD.to_string(),
            metafield(FIELDS_FIELD, None, &fields_type.type_name()),
        );

        // Finally register the metatype for this schema.
        let schema_name = self.type_name();
        let metatype = metaobject(&schema_name, Some(""), fields);
        registry.types.insert(self.type_name(), metatype);
    }
}
