// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{InputObject, InputValue};
use p2panda_rs::schema::Schema;

use crate::graphql::utils::{graphql_input, fields_input_name};

pub fn build_fields_input_object(schema: &Schema) -> InputObject {
    let schema_field_name = fields_input_name(schema.id());
    let mut fields_input = InputObject::new(&schema_field_name);

    for (name, field_type) in schema.fields().iter() {
        fields_input = fields_input.field(InputValue::new(name, graphql_input(field_type)));
    }

    fields_input
}
