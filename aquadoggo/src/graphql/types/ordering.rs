// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::Enum;
use dynamic_graphql::Enum;
use p2panda_rs::schema::Schema;

use crate::graphql::utils::order_by_name;

#[derive(Enum, Debug)]
pub enum OrderDirection {
    Asc,
    Desc,
}

pub struct OrderBy;

impl OrderBy {
    pub fn build(schema: &Schema) -> Enum {
        let mut input_values = Enum::new(order_by_name(schema.id())).item("OWNER");
        for (name, _) in schema.fields().iter() {
            input_values = input_values.item(name.to_uppercase())
        }
        input_values
    }
}
