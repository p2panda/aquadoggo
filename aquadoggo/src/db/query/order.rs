// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::query::{Field, MetaField};

#[derive(Debug, Clone)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub struct Order {
    field: Field,
    direction: Direction,
}

impl Default for Order {
    fn default() -> Self {
        Self {
            field: Field::Meta(MetaField::DocumentId),
            direction: Direction::Ascending,
        }
    }
}
