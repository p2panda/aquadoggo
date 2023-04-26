// SPDX-License-Identifier: AGPL-3.0-or-later

//! Input value types used when specifying ordering parameters on collection queries.
use async_graphql::dynamic::Enum;
use dynamic_graphql::Enum;
use p2panda_rs::schema::Schema;

use crate::graphql::utils::order_by_name;

/// Meta fields by which a collection of documents can be sorted.
// @TODO: Add more fields, see related issue: https://github.com/p2panda/aquadoggo/issues/326
pub const META_ORDER_FIELDS: [&str; 2] = ["DOCUMENT_ID", "DOCUMENT_VIEW_ID"];

/// Possible ordering direction for collection queries.
#[derive(Enum, Debug)]
pub enum OrderDirection {
    #[graphql(name = "ASC")]
    Ascending,

    #[graphql(name = "DESC")]
    Descending,
}

/// Dynamically build an enum input value which can be set to meta or application fields which a
/// collection of documents can be ordered by.
// @TODO: Distinct between enum and application field values. See related issue:
// https://github.com/p2panda/aquadoggo/issues/333
pub fn build_order_enum_value(schema: &Schema) -> Enum {
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
        input_values = input_values.item(name)
    }
    input_values
}
