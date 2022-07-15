// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::Schema;

use crate::graphql::client::types::utils::{metafield, metaobject};
use crate::graphql::client::types::SchemaFieldsType;

pub struct SchemaType(&'static Schema);

impl SchemaType {
    pub fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    pub fn name(&self) -> String {
        self.0.id().as_str()
    }

    pub fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Returns the root metatype for a schema.
    pub fn metatype(&self) -> MetaType {
        let mut fields = IndexMap::new();

        fields.insert(
            "meta".to_string(),
            metafield(
                "meta",
                Some("Metadata for documents of this schema."),
                "DocumentMeta",
            ),
        );

        let fields_type = SchemaFieldsType::new(self.schema());
        fields.insert(
            "fields".to_string(),
            metafield("fields", None, &fields_type.name()),
        );

        metaobject(&self.name(), Some(self.0.description()), fields)
    }

    pub fn metafield(&self) -> MetaField {
        metafield(&self.name(), Some(self.0.description()), &self.name())
    }

    pub fn get_fields_type(&self) -> SchemaFieldsType {
        SchemaFieldsType::new(self.schema())
    }
}
