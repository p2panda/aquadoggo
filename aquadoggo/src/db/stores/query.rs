// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are batched via cursor-based pagination.
use p2panda_rs::document::Document;

use crate::db::errors::QueryError;
use crate::db::query::{Filter, Order, Pagination};
use crate::db::SqlStore;

#[derive(Default, Debug, Clone)]
pub struct Find {
    pub filter: Filter,
    pub order: Order,
}

#[derive(Default, Debug, Clone)]
pub struct FindMany {
    pub pagination: Pagination,
    pub filter: Filter,
    pub order: Order,
}

impl SqlStore {
    pub async fn find(&self, args: &Find) -> Result<Document, QueryError> {
        todo!();
    }

    pub async fn find_many(&self, args: &FindMany) -> Result<Vec<Document>, QueryError> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination};

    use super::{Find, FindMany};

    #[test]
    fn find_getters_and_setters() {
        let order = Order::new(&Field::Meta(MetaField::Owner), &Direction::Descending);

        let query = Find {
            order: order.clone(),
            ..Find::default()
        };

        assert_eq!(query.order, order);
        assert_eq!(query.filter, Filter::default());
    }

    #[test]
    fn find_many_getters_and_setters() {
        let order = Order::new(&Field::Meta(MetaField::Owner), &Direction::Descending);
        let pagination = Pagination::new(&NonZeroU64::new(50).unwrap(), Some(&"cursor".into()));

        let query = FindMany {
            order: order.clone(),
            pagination: pagination.clone(),
            ..FindMany::default()
        };

        assert_eq!(query.pagination, pagination);
        assert_eq!(query.order, order);
        assert_eq!(query.filter, Filter::default());
    }
}
