// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use sqlx::query_scalar;

use p2panda_rs::document::DocumentId;
use p2panda_rs::hash::Hash;
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::stores::StorageEntry;
use crate::db::stores::StorageLog;
use crate::db::Pool;
use crate::errors::StorageProviderResult;
use crate::graphql::client::{
    EntryArgsRequest, EntryArgsResponse, PublishEntryRequest, PublishEntryResponse,
};

use super::errors::DocumentStorageError;

#[derive(Clone)]
pub struct SqlStorage {
    pub(crate) pool: Pool,
}

impl SqlStorage {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// A `StorageProvider` implementation based on `sqlx` that supports SQLite and PostgreSQL databases.
#[async_trait]
impl StorageProvider<StorageEntry, StorageLog> for SqlStorage {
    type EntryArgsResponse = EntryArgsResponse;
    type EntryArgsRequest = EntryArgsRequest;
    type PublishEntryResponse = PublishEntryResponse;
    type PublishEntryRequest = PublishEntryRequest;

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document id by looking up the log
    /// that the entry was stored in.
    async fn get_document_by_entry(
        &self,
        entry_hash: &Hash,
    ) -> StorageProviderResult<Option<DocumentId>> {
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

impl SqlStorage {
    pub async fn get_parents_with_pinned_relation(
        &self,
        document_view_id: &DocumentViewId,
    ) -> StorageProviderResult<Vec<DocumentViewId>> {
        let relation_ids = query_scalar(
            "
                WITH parents_pinned_relation (document_view_id, relation_type, value) AS (
                    SELECT
                        document_view_fields.document_view_id,
                        operation_fields_v1.field_type,
                        operation_fields_v1.value
                    FROM
                        operation_fields_v1
                    LEFT JOIN document_view_fields
                        ON
                            document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name
                    WHERE
                        operation_fields_v1.value = $1 AND operation_fields_v1.field_type LIKE 'pinned_relation%'
                )
                SELECT document_view_id FROM parents_pinned_relation;
            ",
        )
        .bind(document_view_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?
        .iter().map(|id: &String| id.parse().unwrap() ).collect();

        Ok(relation_ids)
    }

    pub async fn get_parents_with_unpinned_relation(
        &self,
        document_view_id: &DocumentId,
    ) -> StorageProviderResult<Vec<DocumentViewId>> {
        let relation_ids: Vec<DocumentViewId> = query_scalar(
            "
                WITH parents_unpinned_relation (document_view_id, relation_type, value) AS (
                    SELECT
                        document_view_fields.document_view_id,
                        operation_fields_v1.field_type,
                        operation_fields_v1.value
                    FROM
                        operation_fields_v1
                    LEFT JOIN document_view_fields
                        ON
                            document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name
                    LEFT JOIN documents
                        ON
                            documents.document_view_id = document_view_fields.document_view_id
                    WHERE
                        operation_fields_v1.value = $1 AND operation_fields_v1.field_type LIKE 'relation%'
                )
                SELECT document_view_id FROM parents_unpinned_relation;
            ",
        )
        .bind(document_view_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?
        .iter().map(|id: &String| id.parse().unwrap() ).collect();

        Ok(relation_ids)
    }
}
