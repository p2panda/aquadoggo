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

#[cfg(test)]
mod tests {
    use crate::db::query::{Field, MetaField};

    use super::{Direction, Order};

    #[test]
    fn default_specification() {
        // If no ordering is selected the documents will be ordered by document id, ascending
        let order = Order::default();

        assert_eq!(
            order,
            Order::new(&Field::Meta(MetaField::DocumentId), &Direction::Ascending)
        )
    }
}
