// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{InputObject, InputValue, TypeRef};
use p2panda_rs::schema::Schema;

use crate::graphql::utils::filter_name;

/// GraphQL object which represents a filter input type which contains a filter object for every
/// field on the passed p2panda schema.
///  
/// A type is added to the root GraphQL schema for every filter, as these types
/// are not known at compile time we make use of the `async-graphql ` `dynamic` module.
pub struct FilterInput;

impl FilterInput {
    /// Build a filter input object for a p2panda schema. It can be used to filter results based
    /// on field values when querying for documents of this schema.
    pub fn build(schema: &Schema) -> InputObject {
        // Construct the document fields object which will be named `<schema_id>Filter`.
        let schema_field_name = filter_name(schema.id());
        let mut filter_input = InputObject::new(&schema_field_name);

        // For every field in the schema we create a type with a resolver.
        for (name, field_type) in schema.fields().iter() {
            match field_type {
                p2panda_rs::schema::FieldType::Boolean => {
                    filter_input =
                        filter_input.field(InputValue::new(name, TypeRef::named("BooleanFilter")));
                }
                p2panda_rs::schema::FieldType::Integer => {
                    filter_input =
                        filter_input.field(InputValue::new(name, TypeRef::named("IntegerFilter")));
                }
                p2panda_rs::schema::FieldType::Float => {
                    filter_input =
                        filter_input.field(InputValue::new(name, TypeRef::named("FloatFilter")));
                }
                p2panda_rs::schema::FieldType::String => {
                    filter_input =
                        filter_input.field(InputValue::new(name, TypeRef::named("StringFilter")));
                }
                p2panda_rs::schema::FieldType::Relation(_) => {
                    filter_input =
                        filter_input.field(InputValue::new(name, TypeRef::named("RelationFilter")));
                }
                p2panda_rs::schema::FieldType::RelationList(_) => {
                    filter_input = filter_input
                        .field(InputValue::new(name, TypeRef::named("RelationListFilter")));
                }
                p2panda_rs::schema::FieldType::PinnedRelation(_) => {
                    filter_input = filter_input.field(InputValue::new(
                        name,
                        TypeRef::named("PinnedRelationFilter"),
                    ));
                }
                p2panda_rs::schema::FieldType::PinnedRelationList(_) => {
                    filter_input = filter_input.field(InputValue::new(
                        name,
                        TypeRef::named("PinnedRelationListFilter"),
                    ));
                }
            };
        }

        filter_input
    }
}
