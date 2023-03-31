// SPDX-License-Identifier: AGPL-3.0-or-later

//! Types used as inputs when specifying ordering parameters on collection queries.
use async_graphql::dynamic::Enum;
use dynamic_graphql::Enum;
use p2panda_rs::schema::Schema;

use crate::graphql::utils::order_by_name;

/// Meta fields by which a collection of documents can be sorted.
pub const META_ORDER_FIELDS: [&str; 3] = ["OWNER", "DOCUMENT_ID", "DOCUMENT_VIEW_ID"];

/// Possible ordering direction for collection queries.
#[derive(Enum, Debug)]
pub enum OrderDirection {
    #[graphql(name = "ASC")]
    Ascending,

    #[graphql(name = "DESC")]
    Descending,
}

/// A constructor for dynamically building an enum type containing all meta and application fields
/// which a collection of documents can be ordered by.
pub struct OrderBy;

impl OrderBy {
    pub fn build(schema: &Schema) -> Enum {
        let mut input_values = Enum::new(order_by_name(schema.id()));

        // Add meta fields to ordering enum.
        //
        // Meta fields are uppercase formatted strings.
        for name in META_ORDER_FIELDS {
            input_values = input_values.item(name)
        }

        // Add document fields to ordering enum.
        //
        // Application fields are lowercase formatted strings.
        for (name, _) in schema.fields().iter() {
            input_values = input_values.item(name.to_lowercase())
        }
        input_values
    }
}
