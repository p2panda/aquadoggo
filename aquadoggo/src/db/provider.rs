// SPDX-License-Identifier: AGPL-3.0-or-later
use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use sqlx::query_as;
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
use super::models::RelationRow;

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
    pub async fn get_document_view_dependencies(
        &self,
        document_view_id: &DocumentViewId,
    ) -> StorageProviderResult<Vec<RelationRow>> {
        let relation_ids = query_as::<_, RelationRow>(
            "
                WITH RECURSIVE cte_relation (document_id, document_view_id, operation_id, relation_type, value) AS (
                    SELECT
                        documents.document_id,
                        document_view_fields.document_view_id,
                        document_view_fields.operation_id,
                        operation_fields_v1.field_type,
                        operation_fields_v1.value
                    FROM
                        document_view_fields
                    LEFT JOIN operation_fields_v1
                        ON
                            operation_fields_v1.operation_id = document_view_fields.operation_id
                        AND
                            operation_fields_v1.name = document_view_fields.name
                    LEFT JOIN documents
                        ON
                            documents.document_view_id = document_view_fields.document_view_id
                    WHERE
                        document_view_fields.document_view_id = $1 AND operation_fields_v1.field_type LIKE '%relation%'

                    UNION

                    SELECT
                        documents.document_id,
                        document_view_fields.document_view_id,
                        document_view_fields.operation_id,
                        operation_fields_v1.field_type,
                        operation_fields_v1.value
                    FROM
                        document_view_fields
                    LEFT JOIN operation_fields_v1
                        ON
                            operation_fields_v1.operation_id = document_view_fields.operation_id
                        AND
                            operation_fields_v1.name = document_view_fields.name
                    LEFT JOIN documents
                        ON
                            documents.document_view_id = document_view_fields.document_view_id
                    JOIN cte_relation
                        ON
                            cte_relation.value = document_view_fields.document_view_id
                        OR
                            cte_relation.value = documents.document_id
                        OR
                            cte_relation.document_id = operation_fields_v1.value
                        OR
                            cte_relation.document_view_id = operation_fields_v1.value
                )

                SELECT relation_type, value FROM cte_relation
                WHERE cte_relation.relation_type LIKE '%relation%';
            ",
        )
        .bind(document_view_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        Ok(relation_ids)
    }
}
