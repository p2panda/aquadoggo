// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::Schema;

use crate::graphql::client::dynamic_object::DynamicObject;
use crate::graphql::client::types::utils::{metafield, metaobject};
use crate::graphql::client::types::DocumentFieldsType;

/// Represents a p2panda schema, which appears in GraphQL as a type that can be queried for
/// documents.
pub struct DocumentType(&'static Schema);

impl DocumentType {
    /// Get the type struct for fields of this schema.
    pub fn get_fields_type(&self) -> DocumentFieldsType {
        DocumentFieldsType::new(self.schema())
    }
}

impl DynamicObject for DocumentType {
    fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    /// Access the schema's name.
    fn name(&self) -> String {
        self.schema().id().as_str()
    }

    /// Access the inner schema.
    fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Returns the root metatype for a schema.
    fn metatype(&self) -> MetaType {
        let mut fields = IndexMap::new();

        fields.insert(
            "meta".to_string(),
            metafield(
                "meta",
                Some("Metadata for documents of this schema."),
                "DocumentMeta",
            ),
        );

        let fields_metafield = self.get_fields_type().metafield();
        fields.insert(fields_metafield.name.clone(), fields_metafield);

        metaobject(&self.name(), Some(self.schema().description()), fields)
    }

    /// Get the metafield for querying this schema.
    fn metafield(&self) -> MetaField {
        metafield(
            &self.name(),
            Some(self.schema().description()),
            &self.name(),
        )
    }
}
