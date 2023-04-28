// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::query::Field;

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
    pub field: Option<Field>,
    pub direction: Direction,
}

impl Order {
    /// Returns a new instance of ordering settings.
    #[allow(dead_code)]
    pub fn new(field: &Field, direction: &Direction) -> Self {
        Self {
            field: Some(field.clone()),
            direction: direction.clone(),
        }
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            field: None,
            direction: Direction::Ascending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Direction, Order};

    #[test]
    fn default_specification() {
        // If no ordering is selected the documents will be ordered by document id, ascending
        let order = Order::default();

        assert_eq!(
            order,
            Order {
                field: None,
                direction: Direction::Ascending
            }
        )
    }
}
