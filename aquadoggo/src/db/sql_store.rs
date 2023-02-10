// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::Pool;

/// SQL based persistent storage that implements `EntryStore`, `OperationStore`, `LogStore` and `DocumentStore`.
#[derive(Clone, Debug)]
pub struct SqlStore {
    pub(crate) pool: Pool,
}

impl SqlStore {
    /// Create a new `SqlStore` using the provided db `Pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}
