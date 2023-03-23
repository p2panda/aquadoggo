// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are batched via cursor-based pagination.
use p2panda_rs::document::DocumentViewFields;
use p2panda_rs::storage_provider::error::DocumentStorageError;

use crate::db::query::{Cursor, Filter, Order, Pagination, Select};
use crate::db::SqlStore;

#[derive(Default, Debug, Clone)]
pub struct Query<C>
where
    C: Cursor,
{
    pub pagination: Pagination<C>,
    pub select: Select,
    pub filter: Filter,
    pub order: Order,
}

impl SqlStore {
    pub async fn query<C: Cursor>(
        &self,
        args: &Query<C>,
    ) -> Result<Vec<DocumentViewFields>, DocumentStorageError> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination};

    use super::Query;

    #[test]
    fn create_find_many_query() {
        let order = Order::new(&Field::Meta(MetaField::Owner), &Direction::Descending);
        let pagination =
            Pagination::<String>::new(&NonZeroU64::new(50).unwrap(), Some(&"cursor".into()));

        let query = Query {
            order: order.clone(),
            pagination: pagination.clone(),
            ..Query::default()
        };

        assert_eq!(query.pagination, pagination);
        assert_eq!(query.order, order);
        assert_eq!(query.filter, Filter::default());
    }
}
