// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::Schema;

use crate::graphql::client::dynamic_object::DynamicObject;
use crate::graphql::client::types::utils::{graphql_typename, metafield, metaobject};

pub struct DocumentFieldsType(&'static Schema);

impl DynamicObject for DocumentFieldsType {
    fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    fn name(&self) -> String {
        format!("{}Fields", self.schema().id().as_str())
    }

    fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Get the metafield for querying this field type.
    fn metafield(&self) -> MetaField {
        metafield("fields", None, &self.name())
    }

    /// Returns the metatype for a schema's fields.
    fn metatype(&self) -> MetaType {
        let mut fields = IndexMap::new();
        self.0.fields().iter().for_each(|(field_name, field_type)| {
            fields.insert(
                field_name.to_string(),
                metafield(field_name, None, &graphql_typename(field_type)),
            );
        });
        metaobject(
            &self.name(),
            Some("Data fields available on documents of this schema."),
            fields,
        )
    }
}
