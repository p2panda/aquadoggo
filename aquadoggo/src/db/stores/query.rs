// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are paginated.

use p2panda_rs::document::Document;

use crate::db::errors::QueryError;
use crate::db::query::{Filter, Order, Pagination};
use crate::db::SqlStore;

#[derive(Debug, Clone)]
pub struct Find {
    filter: Filter,
    order: Order,
}

#[derive(Default, Debug, Clone)]
pub struct FindMany {
    pagination: Pagination,
    filter: Filter,
    order: Order,
}

impl FindMany {
    pub fn new(pagination: Pagination, filter: Filter, order: Order) -> Self {
        Self {
            pagination,
            filter,
            order,
        }
    }
}

impl SqlStore {
    pub async fn find(&self, args: &Find) -> Result<Document, QueryError> {
        todo!();
    }

    pub async fn find_many(&self, args: &FindMany) -> Result<Vec<Document>, QueryError> {
        todo!();
    }
}
