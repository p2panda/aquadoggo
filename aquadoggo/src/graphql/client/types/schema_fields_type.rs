// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::MetaType;
use p2panda_rs::schema::Schema;

use crate::graphql::client::types::utils::{graphql_typename, metafield, metaobject};

pub struct SchemaFieldsType(&'static Schema);

impl SchemaFieldsType {
    pub fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    pub fn name(&self) -> String {
        format!("{}Fields", self.schema().id().as_str())
    }

    pub fn schema(&self) -> &'static Schema {
        self.0
    }

    /// Returns the metatype for a schema's fields.
    pub fn metatype(&self) -> MetaType {
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
