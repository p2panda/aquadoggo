// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use p2panda_rs::schema::Schema;

use crate::graphql::client::dynamic_types::utils::{graphql_typename, metafield, metaobject};

pub struct DocumentFieldsType(&'static Schema);

impl DocumentFieldsType {
    pub fn new(schema: &'static Schema) -> Self {
        Self(schema)
    }

    pub fn schema(&self) -> &'static Schema {
        self.0
    }

    pub fn type_name(&self) -> String {
        format!("{}Fields", self.schema().id().as_str())
    }

    pub fn register_type(&self, registry: &mut async_graphql::registry::Registry) {
        let mut fields = IndexMap::new();

        // Create a GraphQL field for every schema field.
        self.0.fields().iter().for_each(|(field_name, field_type)| {
            fields.insert(
                field_name.to_string(),
                metafield(field_name, None, &graphql_typename(field_type)),
            );
        });

        // Create a meta object with the fields defined above and insert it into the registry.
        registry.types.insert(
            self.type_name(),
            metaobject(
                &self.type_name(),
                Some("Data fields available on documents of this schema."),
                fields,
            ),
        );
    }
}
