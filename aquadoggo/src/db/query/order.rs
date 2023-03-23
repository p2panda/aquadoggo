// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::query::{Field, MetaField};

/// Options to determine the direction of the ordering.
#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    /// Arrange items from smallest to largest value.
    Ascending,

    /// Arrange items from largest to smallest value.
    Descending,
}

/// Ordering settings which can be used further to construct a database query.
///
/// An ordering determines in which direction and based on what field the results are sorted.
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub field: Field,
    pub direction: Direction,
}

impl Order {
    /// Returns a new instance of ordering settings.
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