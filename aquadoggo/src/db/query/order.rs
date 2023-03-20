// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::query::{Field, MetaField};

#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub field: Field,
    pub direction: Direction,
}

impl Order {
    pub fn new(field: &Field, direction: &Direction) -> Self {
        Self {
            field: field.clone(),
            direction: direction.clone(),
        }
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            field: Field::Meta(MetaField::DocumentId),
            direction: Direction::Ascending,
        }
    }
}
