// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are paginated.
use p2panda_rs::document::Document;

use crate::db::errors::QueryError;
use crate::db::SqlStore;

impl SqlStore {
    pub async fn find() -> Result<Document, QueryError> {
        todo!();
    }

    pub async fn find_many() -> Result<Vec<Document>, QueryError> {
        todo!();
    }
}
