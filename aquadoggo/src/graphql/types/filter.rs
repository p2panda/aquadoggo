// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{InputObject, InputValue, TypeRef};
use dynamic_graphql::InputObject;
use p2panda_rs::schema::Schema;

use crate::graphql::utils::filter_name;

/// A filter input type for string field values.
#[derive(InputObject)]
pub struct StringFilter {
    /// Filter by values in set.
    #[graphql(name = "in")]
    is_in: Option<Vec<String>>,

    /// Filter by values not in set.
    #[graphql(name = "notIn")]
    is_not_in: Option<Vec<String>>,

    /// Filter by equal to.
    #[graphql(name = "eq")]
    eq: Option<String>,

    /// Filter by not equal to.
    #[graphql(name = "notEq")]
    not_eq: Option<String>,

    /// Filter by greater than or equal to.
    gte: Option<String>,

    /// Filter by greater than.
    gt: Option<String>,

    /// Filter by less than or equal to.
    lte: Option<String>,

    /// Filter by less than.
    lt: Option<String>,

    /// Filter for items which contain given string.
    contains: Option<String>,

    /// Filter for items which don't contain given string.
    #[graphql(name = "notContains")]
    not_contains: Option<String>,
}

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
        for (name, _field_type) in schema.fields().iter() {
            filter_input =
                filter_input.field(InputValue::new(name, TypeRef::named("StringFilter")));
        }

        filter_input
    }
}
