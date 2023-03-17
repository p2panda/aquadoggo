// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are paginated.
use std::num::NonZeroU64;

use libp2p::identity::PublicKey;
use p2panda_rs::document::Document;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::FieldName;

use crate::db::errors::QueryError;
use crate::db::SqlStore;

const DEFAULT_PAGE_SIZE: u64 = 10;

pub type Cursor = String;

#[derive(Debug)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug)]
pub struct Order {
    field_name: FieldName,
    direction: Direction,
}

#[derive(Debug)]
pub struct Pagination {
    first: NonZeroU64,
    after: Option<Cursor>,
}

impl Pagination {
    pub fn new(first: NonZeroU64, after: Option<&Cursor>) -> Self {
        Self {
            first,
            after: after.cloned(),
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            // Unwrap here because we know that the default is non-zero
            first: NonZeroU64::new(DEFAULT_PAGE_SIZE).unwrap(),
            after: None,
        }
    }
}

#[derive(Debug)]
pub struct FilterMeta {
    public_keys: Option<Vec<PublicKey>>,
    edited: Option<bool>,
    deleted: Option<bool>,
}

#[derive(Debug)]
pub struct FilterFields {
    field_name: FieldName,
    eq: Option<Vec<OperationValue>>,
    gt: Option<Vec<OperationValue>>,
    gte: Option<Vec<OperationValue>>,
    lt: Option<Vec<OperationValue>>,
    lte: Option<Vec<OperationValue>>,
}

#[derive(Debug)]
pub struct Filter {
    meta: Option<FilterMeta>,
    fields: Option<Vec<FilterFields>>,
}

#[derive(Debug)]
pub struct FindQuery {
    order: Option<Order>,
    filter: Option<Filter>,
}

#[derive(Debug)]
pub struct FindManyQuery {
    order: Option<Order>,
    filter: Option<Filter>,
    pagination: Pagination,
}

impl SqlStore {
    pub async fn find(&self, args: &FindQuery) -> Result<Document, QueryError> {
        todo!();
    }

    pub async fn find_many(&self, args: &FindManyQuery) -> Result<Vec<Document>, QueryError> {
        todo!();
    }
}
