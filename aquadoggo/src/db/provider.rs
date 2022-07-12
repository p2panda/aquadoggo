// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::VerifiedOperation;
use p2panda_rs::storage_provider::traits::StorageProvider;
use sqlx::query_scalar;

use crate::db::request::{EntryArgsRequest, PublishEntryRequest};
use crate::db::stores::{StorageEntry, StorageLog};
use crate::db::Pool;
use crate::errors::Result;
use crate::graphql::client::NextEntryArguments;

/// Sql based storage that implements `StorageProvider`.
#[derive(Clone, Debug)]
pub struct SqlStorage {
    pub(crate) pool: Pool,
}

impl SqlStorage {
    /// Create a new `SqlStorage` using the provided db `Pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// A `StorageProvider` implementation based on `sqlx` that supports SQLite and PostgreSQL
/// databases.
#[async_trait]
impl StorageProvider<StorageEntry, StorageLog, VerifiedOperation> for SqlStorage {
    type EntryArgsResponse = NextEntryArguments;
    type EntryArgsRequest = EntryArgsRequest;
    type PublishEntryResponse = NextEntryArguments;
    type PublishEntryRequest = PublishEntryRequest;

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document id by looking up the log
    /// that the entry was stored in.
    async fn get_document_by_entry(&self, entry_hash: &Hash) -> Result<Option<DocumentId>> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                logs.document
            FROM
                logs
            INNER JOIN entries
                ON (logs.log_id = entries.log_id
                    AND logs.author = entries.author)
            WHERE
                entries.entry_hash = $1
            ",
        )
        .bind(entry_hash.as_str())
        .fetch_optional(&self.pool)
        .await?;

        // Unwrap here since we validate hashes before storing them in the db.
        let hash = result.map(|str| {
            Hash::new(&str)
                .expect("Corrupt hash found in database")
                .into()
        });

        Ok(hash)
    }
}
