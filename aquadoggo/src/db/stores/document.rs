// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module implements `DocumentStore` on `SqlStore` as well as aditional insertion methods
//! specific to the `aquadoggo` storage patterns. The resulting interface offers all storage
//! methods used for persisting and retrieving materialised documents.
//!
//! Documents are created and mutated via operations which arrive at a node. Once validated, the
//! new operations are sent straight to the materialiser service which builds the documents
//! themselves. On completion, the resultant documents are stored and can be retrieved using the
//! methods defined here.
//!
//! The whole document store can be seen as a live cache. All its content is derived from
//! operations already stored on the node. It allows easy and quick access to current or pinned
//! values.
//!
//! Documents are stored in the database in three tables. These are `documents`, `document_views`
//! and `document_view_fields`. A `document` can have many `document_views`, one showing the
//! current state and any number of historic views. A `document_view` itself a unique id plus one
//! or many `document_view_fields` which are pointers to the operation holding the current value
//! for the documents' field.
//!
//! As mentioned above, a useful property of documents is that they make it easy to retain past
//! state, we call these states document views. When a document is updated it gets a new state, or
//! view, which can be referred to by a globally unique document view id.
//!
//! The getter methods allow retrieving a document by its `DocumentId` or its `DocumentViewId`. The
//! former always returns the most current document state, the latter returns the specific document
//! view if it has already been materialised and stored. Although it is possible to construct a
//! document at any point in its history if all operations are retained, we use a system of "pinned
//! relations" to identify and materialise only views we explicitly wish to keep.
use async_trait::async_trait;
use log::debug;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;
use sqlx::any::AnyQueryResult;
use sqlx::{query, query_as, query_scalar, Any, Transaction};

use crate::db::models::utils::parse_document_view_field_rows;
use crate::db::models::{DocumentRow, DocumentViewFieldRow};
use crate::db::types::StorageDocument;
use crate::db::Pool;
use crate::db::SqlStore;

#[async_trait]
impl DocumentStore for SqlStore {
    type Document = StorageDocument;

    /// Get a document from the store by its `DocumentId`.
    ///
    /// Retrieves a document in its most current state from the store. Ignores documents which
    /// contain a DELETE operation.
    ///
    /// An error is returned only if a fatal database error occurs.
    async fn get_document(
        &self,
        id: &DocumentId,
    ) -> Result<Option<Self::Document>, DocumentStorageError> {
        // Retrieve one row from the document table matching on the passed id.
        let document_row = query_as::<_, DocumentRow>(
            "
            SELECT
                documents.document_id,
                documents.document_view_id,
                documents.schema_id,
                operations_v1.public_key,
                documents.is_deleted
            FROM
                documents
            LEFT JOIN operations_v1
                ON
                    operations_v1.operation_id = $1
            WHERE
                documents.document_id = $1 AND documents.is_deleted = false
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // If no row matched we return None here, otherwise unwrap safely.
        let document_row = match document_row {
            Some(document_row) => document_row,
            None => return Ok(None),
        };

        // We now want to retrieve the view (current key-value map) for this document, as we
        // already filtered out deleted documents in the query above we can expect all documents
        // we handle here to have an associated view in the database.
        let document_view_id = document_row.document_view_id.parse().unwrap();
        let document_view_field_rows =
            get_document_view_field_rows(&self.pool, &document_view_id).await?;
        // this method assumes all values coming from the db are already validated and so
        // unwraps where errors might occur.
        let document_view_fields = Some(parse_document_view_field_rows(document_view_field_rows));

        // Construct a `StorageDocument` based on the retrieved values.
        let document = StorageDocument {
            id: id.to_owned(),
            view_id: document_view_id,
            schema_id: document_row.schema_id.parse().unwrap(),
            fields: document_view_fields,
            author: document_row.public_key.parse().unwrap(),
            deleted: document_row.is_deleted,
        };

        Ok(Some(document))
    }

    /// Get a document from the database by `DocumentViewId`.
    ///
    /// Get's a document at a specific point in its history. Only returns views that have already
    /// been materialised and persisted in the store. These are likely to be "pinned views" which
    /// are relations from other documents, in which case the materialiser service will have
    /// identified and materialised them ready for querying.
    ///
    /// Any view which existed as part of a document which is now deleted is ignored.
    ///
    /// An error is returned only if a fatal database error occurs.
    async fn get_document_by_view_id(
        &self,
        view_id: &DocumentViewId,
    ) -> Result<Option<StorageDocument>, DocumentStorageError> {
        // Retrieve the id of the document which the passed view id comes from.
        let document_id: Option<String> = query_scalar(
            "
            SELECT
                document_id
            FROM
                document_views
            WHERE
                document_view_id = $1
            ",
        )
        .bind(view_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Parse the document id if one was found otherwise we can already return None here as no
        // document for the passed view could be found.
        let document_id: DocumentId = match document_id {
            Some(document_id) => document_id.parse().unwrap(),
            None => return Ok(None),
        };

        // Get a row for the document matching to the found document id.
        let document_row = query_as::<_, DocumentRow>(
            "
            SELECT
                documents.document_id,
                documents.document_view_id,
                documents.schema_id,
                operations_v1.public_key,
                documents.is_deleted
            FROM
                documents
            LEFT JOIN operations_v1
                ON
                    operations_v1.operation_id = $1
            WHERE
                documents.document_id = $1 AND documents.is_deleted = false
            ",
        )
        .bind(document_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // If no row matched we return None here, otherwise unwrap safely
        let document_row = match document_row {
            Some(document_row) => document_row,
            // Document might have already been deleted
            None => return Ok(None),
        };

        // We now want to retrieve the view (current key-value map) for this document, as we
        // already filtered out deleted documents in the query above we can expect all documents
        // we handle here to have an associated view in the database.
        let document_view_field_rows = get_document_view_field_rows(&self.pool, view_id).await?;

        // This method assumes all values coming from the db are already validated and so
        // unwraps where errors might occur.
        let document_view_fields = Some(parse_document_view_field_rows(document_view_field_rows));

        // Construct a `StorageDocument` based on the retrieved values
        let document = StorageDocument {
            id: document_row.document_id.parse().unwrap(),
            view_id: view_id.to_owned(), // Set to requested document view id, not the current
            schema_id: document_row.schema_id.parse().unwrap(),
            fields: document_view_fields,
            author: document_row.public_key.parse().unwrap(),
            deleted: document_row.is_deleted,
        };

        Ok(Some(document))
    }

    /// Get all documents which follow the passed schema id.
    ///
    /// Retrieves all documents, with their most current views, which follow the specified schema.
    /// Deleted documents are not included.
    ///
    /// An error is returned only if a fatal database error occurs.
    async fn get_documents_by_schema(
        &self,
        schema_id: &SchemaId,
    ) -> Result<Vec<Self::Document>, DocumentStorageError> {
        // Retrieve all rows from the document table where the passed schema_id matches.
        let document_rows = query_as::<_, DocumentRow>(
            "
            SELECT
                documents.document_id,
                documents.document_view_id,
                documents.schema_id,
                operations_v1.public_key,
                documents.is_deleted
            FROM
                documents
            LEFT JOIN operations_v1
                ON
                    operations_v1.operation_id = documents.document_id
            WHERE
                documents.schema_id = $1 AND documents.is_deleted = false
            ",
        )
        .bind(schema_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // If no rows were found we can already return an empty vec here.
        if document_rows.is_empty() {
            return Ok(vec![]);
        }

        // For every row we found we want to retrieve the current view as well.
        let mut documents: Vec<StorageDocument> = vec![];
        for document_row in document_rows {
            let document_view_id = document_row.document_view_id.parse().unwrap();
            // We now want to retrieve the view (current key-value map) for this document, as we
            // already filtered out deleted documents in the query above we can expect all documents
            // we handle here to have an associated view in the database.
            let document_view_field_rows =
                get_document_view_field_rows(&self.pool, &document_view_id).await?;
            // this method assumes all values coming from the db are already validated and so
            // unwraps where errors might occur.
            let document_view_fields =
                Some(parse_document_view_field_rows(document_view_field_rows));

            // Construct a `StorageDocument` based on the retrieved values.
            let document = StorageDocument {
                id: document_row.document_id.parse().unwrap(),
                view_id: document_view_id,
                schema_id: document_row.schema_id.parse().unwrap(),
                fields: document_view_fields,
                author: document_row.public_key.parse().unwrap(),
                deleted: document_row.is_deleted,
            };

            documents.push(document)
        }

        Ok(documents)
    }
}

/// Storage API offering an interface for inserting documents and document views into the database.
///
/// These methods are specific to aquadoggos approach to document caching and are defined outside
/// of the required `DocumentStore` trait.
impl SqlStore {
    /// Insert a document into the database.
    ///
    /// This method inserts or updates a row in the documents table and then inserts the documents
    /// current view and field values into the `document_views` and `document_view_fields` tables
    /// respectively.
    ///
    /// If the document already existed in the store then its current view and view id will be
    /// updated with those contained on the passed document.
    ///
    /// If any of the operations fail all insertions are rolled back.
    ///
    /// An error is returned in the case of a fatal database error.
    ///
    /// Note: "out-of-date" document views will remain in storage when a document already existed
    /// and is updated. If they are not needed for anything else they can be garbage collected.
    pub async fn insert_document(
        &self,
        document: &impl AsDocument,
    ) -> Result<(), DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()` can
        // be rolled back in the event of an error.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Insert the document and view to the database, in the case of an error all insertions
        // since the tx was instantiated above will be rolled back.
        let result = insert_document(&mut tx, document).await;

        match result {
            // Commit the tx here if no error occurred.
            Ok(_) => tx
                .commit()
                .await
                .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string())),
            // Rollback here if an error occurred.
            Err(err) => {
                tx.rollback()
                    .await
                    .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;
                Err(err)
            }
        }
    }

    /// Insert a document view into the database.
    ///
    /// This method performs one insertion in the `document_views` table and at least one in the
    /// `document_view_fields` table. If either of these operations fail then all insertions are
    /// rolled back.
    ///
    /// An error is returned in the case of a fatal storage error.
    pub async fn insert_document_view(
        &self,
        document_view: &DocumentView,
        document_id: &DocumentId,
        schema_id: &SchemaId,
    ) -> Result<(), DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Insert the document view into the `document_views` table. Rollback insertions if an error occurs.
        match insert_document_view(&mut tx, document_view, document_id, schema_id).await {
            Ok(_) => (),
            Err(err) => {
                tx.rollback()
                    .await
                    .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;
                return Err(err);
            }
        };

        // Insert the document view fields into the `document_view_fields` table. Rollback
        // insertions if an error occurs.
        match insert_document_fields(&mut tx, document_view).await {
            Ok(_) => (),
            Err(err) => {
                tx.rollback()
                    .await
                    .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;
                return Err(err);
            }
        };

        // Commit the tx here as no errors occurred.
        tx.commit()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))
    }

    /// Get the ids for all document views for a document which are currently materialized to the store.
    pub async fn get_all_document_view_ids(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<DocumentViewId>, DocumentStorageError> {
        let document_view_ids: Vec<String> = query_scalar(
            "
            SELECT
                document_views.document_view_id
            FROM
                document_views
            WHERE
                document_views.document_id = $1
            ORDER BY
                document_views.document_id
            ",
        )
        .bind(document_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        Ok(document_view_ids
            .iter()
            .map(|document_id_str| {
                document_id_str
                    .parse::<DocumentViewId>()
                    .expect("Document Id's coming from the store should be valid")
            })
            .collect())
    }

    /// Get the ids of all documents which are related to from another document view.
    pub async fn get_child_document_ids(
        &self,
        document_view_id: &DocumentViewId,
    ) -> Result<Vec<DocumentId>, DocumentStorageError> {
        // Collect all ids or view ids of children related to from the passed document view.
        //
        // Value is None when a relation list is empty.
        let children_ids: Vec<Option<String>> = query_scalar(
            "
            SELECT
                operation_fields_v1.value
            FROM
                document_view_fields
            LEFT JOIN
                operation_fields_v1
            ON
                document_view_fields.operation_id = operation_fields_v1.operation_id
            AND
                document_view_fields.name = operation_fields_v1.name
            WHERE
                operation_fields_v1.field_type IN (
                    'pinned_relation',
                    'pinned_relation_list',
                    'relation',
                    'relation_list'
                )
            AND
                document_view_fields.document_view_id = $1
        ",
        )
        .bind(document_view_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        // Remove any None results from the vec.
        let children_ids: Vec<String> = children_ids.into_iter().flatten().collect();

        // If no children were found return now already with an empty vec.
        if children_ids.is_empty() {
            return Ok(vec![]);
        }

        let args = children_ids
            .iter()
            .map(|id| format!("'{id}'"))
            .collect::<Vec<String>>()
            .join(",");

        // Query for any document included in the list of children.
        let document_ids: Vec<String> = query_scalar(&format!(
            "
            SELECT DISTINCT
                document_views.document_id
            FROM
                document_views
            WHERE
                document_views.document_view_id IN ({})
            OR
                document_views.document_id IN ({})
            ORDER BY
                document_views.document_id ASC
            ",
            args, args
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        Ok(document_ids
            .iter()
            .map(|document_id_str| {
                document_id_str
                    .parse::<DocumentId>()
                    .expect("Document Id's coming from the store should be valid")
            })
            .collect())
    }

    /// Attempt to remove a document view from the store. Returns a boolean which indicates if the
    /// removal took place.
    ///
    /// This operations only succeeds if the view is "dangling", meaning no other document view
    /// exists which relates to this view, AND it is not the current view of any document.
    pub async fn prune_document_view(
        &self,
        document_view_id: &DocumentViewId,
    ) -> Result<bool, DocumentStorageError> {
        // Attempt to delete the view. If it is pinned from an existing view, or it is the current
        // view of a document, the deletion will not go ahead.
        let result = query(
                "
                DELETE FROM
                    document_views
                WHERE
                    document_views.document_view_id = $1
                AND NOT EXISTS (
                    SELECT
                        document_view_fields.document_view_id
                    FROM
                        document_view_fields
                    LEFT JOIN
                        operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                    AND
                        document_view_fields.name = operation_fields_v1.name
                    WHERE
                        operation_fields_v1.field_type IN ('pinned_relation', 'pinned_relation_list')
                    AND
                        operation_fields_v1.value = $1
                )
                AND NOT EXISTS (
                    SELECT documents.document_id FROM documents
                    WHERE documents.document_view_id = $1
                )
                "
            )
            .bind(document_view_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        // If any rows were affected the deletion went ahead.
        if result.rows_affected() > 0 {
            debug!("Deleted view: {}", document_view_id);
            Ok(true)
        } else {
            debug!("Did not delete view: {}", document_view_id);
            Ok(false)
        }
    }

    /// Check if this view is the current view of its document.
    pub async fn is_current_view(
        &self,
        document_view_id: &DocumentViewId,
    ) -> Result<bool, DocumentStorageError> {
        let document_view_id: Option<String> = query_scalar(
            "
            SELECT
                documents.document_view_id
            FROM
                documents
            WHERE
                documents.document_view_id = $1
            ",
        )
        .bind(document_view_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        Ok(document_view_id.is_some())
    }

    /// Purge a document from the store by its id.
    ///
    /// This removes entries, operations and any materialized documents which exist.
    ///
    /// The only unaffected table after deletion is the `logs` table as we still want to remember
    /// which log ids an author has already used so we can continue to avoid collisions.
    pub async fn purge_document(
        &self,
        document_id: &DocumentId,
    ) -> Result<(), DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Delete rows from `documents` table, this cascades up to `document_views` and
        // `document_view_fields` tables.
        query(
            "
                DELETE FROM documents
                WHERE documents.document_id = $1
                ",
        )
        .bind(document_id.to_string())
        .fetch_all(&mut tx)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Delete rows from `entries` table.
        query(
            "
                DELETE FROM entries
                WHERE entries.entry_hash IN (
                    SELECT operations_v1.operation_id FROM operations_v1
                    WHERE operations_v1.document_id = $1
                )
                ",
        )
        .bind(document_id.to_string())
        .fetch_all(&mut tx)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Delete rows from `operations_v1` table, this cascades up to `operation_fields_v1` table
        // as well.
        query(
            "
                DELETE FROM operations_v1
                WHERE operations_v1.document_id = $1
                ",
        )
        .bind(document_id.to_string())
        .fetch_all(&mut tx)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Commit the transaction if all queries succeeded.
        tx.commit()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        Ok(())
    }
}

// Helper method for getting rows from the `document_view_fields` table.
async fn get_document_view_field_rows(
    pool: &Pool,
    id: &DocumentViewId,
) -> Result<Vec<DocumentViewFieldRow>, DocumentStorageError> {
    // Get all rows which match against the passed document view id.
    //
    // This query performs a join against the `operation_fields_v1` table as this is where the
    // actual field values live. The `document_view_fields` table defines relations between a
    // document view and the operation values which hold its field values.
    //
    // Each field has one row, or in the case of list values (pinned relations, or relation lists)
    // then one row exists for every item in the list. The `list_index` column is used for
    // consistently ordering list items.
    query_as::<_, DocumentViewFieldRow>(
        "
        SELECT
            document_views.document_id,
            document_view_fields.document_view_id,
            document_view_fields.operation_id,
            document_view_fields.name,
            operation_fields_v1.list_index,
            operation_fields_v1.field_type,
            operation_fields_v1.value
        FROM
            document_view_fields
        LEFT JOIN
            operation_fields_v1
        ON
            document_view_fields.operation_id = operation_fields_v1.operation_id
        AND
            document_view_fields.name = operation_fields_v1.name
        LEFT JOIN
            document_views
        ON
            document_view_fields.document_view_id = document_views.document_view_id
        WHERE
            document_view_fields.document_view_id = $1
        ORDER BY
            operation_fields_v1.list_index ASC
        ",
    )
    .bind(id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))
}

// Helper method for inserting rows in the `document_view_fields` table.
async fn insert_document_fields(
    tx: &mut Transaction<'_, Any>,
    document_view: &DocumentView,
) -> Result<Vec<AnyQueryResult>, DocumentStorageError> {
    let mut results = Vec::with_capacity(document_view.len());

    for (name, value) in document_view.iter() {
        let result = query(
            "
            INSERT INTO
                document_view_fields (
                    document_view_id,
                    operation_id,
                    name
                )
            VALUES
                ($1, $2, $3)
            ",
        )
        .bind(document_view.id().to_string())
        .bind(value.id().as_str().to_owned())
        .bind(name)
        .execute(&mut *tx)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        results.push(result);
    }

    Ok(results)
}

// Helper method for inserting document views into the `document_views` table.
async fn insert_document_view(
    tx: &mut Transaction<'_, Any>,
    document_view: &DocumentView,
    document_id: &DocumentId,
    schema_id: &SchemaId,
) -> Result<AnyQueryResult, DocumentStorageError> {
    query(
        "
        INSERT INTO
            document_views (
                document_view_id,
                document_id,
                schema_id
            )
        VALUES
            ($1, $2, $3)
        ON CONFLICT DO NOTHING
        ",
    )
    .bind(document_view.id().to_string())
    .bind(document_id.to_string())
    .bind(schema_id.to_string())
    .execute(tx)
    .await
    .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))
}

// Helper method for inserting documents into the database. For this, insertions are made in the
// `documents`, `document_views` and `document_view_fields` tables.
async fn insert_document(
    tx: &mut Transaction<'_, Any>,
    document: &impl AsDocument,
) -> Result<(), DocumentStorageError> {
    // Insert or update the document to the `documents` table.
    query(
        "
        INSERT INTO
            documents (
                document_id,
                document_view_id,
                is_deleted,
                schema_id
            )
        VALUES
            ($1, $2, $3, $4)
        ON CONFLICT(document_id) DO UPDATE SET
            document_view_id = $2,
            is_deleted = $3
        ",
    )
    .bind(document.id().as_str())
    .bind(document.view_id().to_string())
    .bind(document.is_deleted())
    .bind(document.schema_id().to_string())
    .execute(&mut *tx)
    .await
    .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

    // If the document is not deleted, then we also want to insert its view and fields.
    if !document.is_deleted() && document.view().is_some() {
        // Construct the view, unwrapping the document view fields as we checked they exist above.
        let document_view =
            DocumentView::new(document.view_id(), document.view().unwrap().fields());

        // Insert the document view
        insert_document_view(
            &mut *tx,
            &document_view,
            document.id(),
            document.schema_id(),
        )
        .await?;

        // Insert the document view fields
        insert_document_fields(&mut *tx, &document_view).await?;
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use p2panda_rs::api::next_args;
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewFields, DocumentViewId};
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::traits::AsOperation;
    use p2panda_rs::operation::{Operation, OperationId};
    use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
    use p2panda_rs::test_utils::constants;
    use p2panda_rs::test_utils::fixtures::{
        key_pair, operation, random_document_id, random_document_view_id, random_operation_id,
    };
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::db::stores::document::DocumentView;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{
        add_schema_and_documents, assert_query, doggo_schema, populate_and_materialize,
        populate_store, populate_store_config, test_runner, PopulateStoreConfig, TestNode,
    };

    #[rstest]
    fn insert_and_get_one_document_view(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document = documents.first().expect("At least one document");

            // Get the operations and build the document.
            let operations = node
                .context
                .store
                .get_operations_by_document_id(document.id())
                .await
                .unwrap();

            // Build the document from the operations.
            let document_builder = DocumentBuilder::from(&operations);
            let (document, _) = document_builder.build().unwrap();

            // Insert the document into the store
            let result = node.context.store.insert_document(&document).await;
            assert!(result.is_ok());

            // Find the "CREATE" operation and get its id.
            let create_operation = WithId::<OperationId>::id(
                operations
                    .iter()
                    .find(|operation| operation.is_create())
                    .unwrap(),
            )
            .to_owned();

            // Build the document with just the first operation.
            let (document_at_view_1, _) = document_builder
                .build_to_view_id(create_operation.into())
                .unwrap();

            // Insert it into the store as well.
            let result = node
                .context
                .store
                .insert_document_view(
                    &document_at_view_1.view().unwrap(),
                    document_at_view_1.id(),
                    document_at_view_1.schema_id(),
                )
                .await;
            assert!(result.is_ok());

            // We should be able to retrieve the document at either of its views now.

            // Here we request the document with its initial state.
            let retrieved_document = node
                .context
                .store
                .get_document_by_view_id(document_at_view_1.view_id())
                .await
                .unwrap()
                .unwrap();

            // The retrieved document views should match the inserted one.
            assert_eq!(retrieved_document.id(), document_at_view_1.id());
            assert_eq!(retrieved_document.view_id(), document_at_view_1.view_id());
            assert_eq!(retrieved_document.fields(), document_at_view_1.fields());

            // Here we request it at its current state.
            let retrieved_document = node
                .context
                .store
                .get_document_by_view_id(document.view_id())
                .await
                .unwrap()
                .unwrap();

            // The retrieved document views should match the inserted one.
            assert_eq!(retrieved_document.id(), document.id());
            assert_eq!(retrieved_document.view_id(), document.view_id());
            assert_eq!(retrieved_document.fields(), document.fields());

            // If we retrieve the document by its id, we expect the current state.
            let retrieved_document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap()
                .unwrap();

            // The retrieved document views should match the inserted one.
            assert_eq!(retrieved_document.id(), document.id());
            assert_eq!(retrieved_document.view_id(), document.view_id());
            assert_eq!(retrieved_document.fields(), document.fields());
        });
    }

    #[rstest]
    fn document_view_does_not_exist(random_document_view_id: DocumentViewId) {
        test_runner(|node: TestNode| async move {
            // We try to retrieve a document view by its id but no view with that id exists.
            let view_does_not_exist = node
                .context
                .store
                .get_document_by_view_id(&random_document_view_id)
                .await
                .unwrap();

            // The return result should contain a none value.
            assert!(view_does_not_exist.is_none());
        });
    }

    #[rstest]
    fn insert_document_view_with_missing_operation(
        #[from(random_operation_id)] operation_id: OperationId,
        #[from(random_document_id)] document_id: DocumentId,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
        operation: Operation,
    ) {
        test_runner(|node: TestNode| async move {
            // Construct a document view from an operation which is not in the database.
            let document_view = DocumentView::new(
                &document_view_id,
                &DocumentViewFields::new_from_operation_fields(
                    &operation_id,
                    &operation.fields().unwrap(),
                ),
            );

            // Inserting the view should fail as it must relate to an
            // operation which is already in the database.
            let result = node
                .context
                .store
                .insert_document_view(&document_view, &document_id, constants::schema().id())
                .await;

            assert!(result.is_err());
        });
    }

    #[rstest]
    fn inserts_gets_document(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document = documents.first().expect("At least one document");

            // The document is successfully inserted into the database, this relies on the
            // operations already being present and would fail if they were not.
            let result = node.context.store.insert_document(document).await;
            assert!(result.is_ok());

            // We can retrieve the most recent document view for this document by its id.
            let retrieved_document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap()
                .unwrap();

            // We can retrieve a specific document view for this document by its view_id. In this
            // case, that should be the same as the view retrieved above.
            let specific_document = node
                .context
                .store
                .get_document_by_view_id(document.view_id())
                .await
                .unwrap()
                .unwrap();

            for key in [
                "username",
                "age",
                "height",
                "is_admin",
                "data",
                "profile_picture",
                "many_profile_pictures",
                "special_profile_picture",
                "many_special_profile_pictures",
                "another_relation_field",
            ] {
                // The values contained in both retrieved document views
                // should match the expected ones.
                assert!(retrieved_document.get(key).is_some());
                assert_eq!(retrieved_document.get(key), document.get(key));
                assert!(specific_document.get(key).is_some());
                assert_eq!(specific_document.get(key), document.get(key));
            }
        });
    }

    #[rstest]
    fn no_view_when_document_deleted(
        #[from(populate_store_config)]
        #[with(10, 1, vec![KeyPair::new()], true)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any
            // resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document = documents.first().expect("At least one document");

            // Get the view id.
            let view_id = document.view_id();

            // As it has been deleted, there should be no view.
            assert!(document.view().is_none());

            // Here we insert the document. This action also sets its most recent view.
            let result = node.context.store.insert_document(document).await;
            assert!(result.is_ok());

            // We retrieve the most recent view for this document by its document id, but as the
            // document is deleted, we should get a none value back.
            let document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap();
            assert!(document.is_none());

            // We also try to retrieve the specific document view by its view id. This should also
            // return none as it is deleted.
            let document = node
                .context
                .store
                .get_document_by_view_id(view_id)
                .await
                .unwrap();
            assert!(document.is_none());
        });
    }

    #[rstest]
    fn get_documents_by_schema_deleted_document(
        #[from(populate_store_config)]
        #[with(10, 1, vec![KeyPair::new()], true)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document = documents.first().expect("At least one document");

            // Insert the document, this is possible even though it has been deleted.
            let result = node.context.store.insert_document(document).await;
            assert!(result.is_ok());

            // When we try to retrieve it by schema id we should NOT get it back.
            let document_views = node
                .context
                .store
                .get_documents_by_schema(doggo_schema().id())
                .await
                .unwrap();
            assert!(document_views.is_empty());
        });
    }

    #[rstest]
    fn updates_a_document(
        #[from(populate_store_config)]
        #[with(10, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let documents = populate_store(&node.context.store, &config).await;
            let document = documents.first().expect("At least one document");

            // Get the operations for this document and sort them into linear order.
            let operations = node
                .context
                .store
                .get_operations_by_document_id(document.id())
                .await
                .unwrap();
            let document_builder = DocumentBuilder::from(&operations);
            let (_, sorted_operations) = document_builder.build().unwrap();

            // We want to test that a document is updated.
            let mut current_operations = Vec::new();
            for operation in sorted_operations {
                // For each operation in the db we insert a document, cumulatively adding the next
                // operation each time. this should perform an "INSERT" first in the documents
                // table, followed by 9 "UPDATES".
                current_operations.push(operation.clone());

                // We build each document.
                let (document, _) = DocumentBuilder::new(current_operations.clone())
                    .build()
                    .expect("Build document");

                // Insert it to the database, this should also update its view.
                node.context
                    .store
                    .insert_document(&document)
                    .await
                    .expect("Insert document");

                // We can retrieve the document by its document id.
                let retrieved_document = node
                    .context
                    .store
                    .get_document(document.id())
                    .await
                    .expect("Get document")
                    .expect("Unwrap document");

                // And also directly by its document view id.
                let specific_document = node
                    .context
                    .store
                    .get_document_by_view_id(document.view_id())
                    .await
                    .expect("Get document")
                    .expect("Unwrap document");

                // The views should equal the current view of the document we inserted.
                // This includes the value and the view id.
                assert_eq!(document.id(), retrieved_document.id());
                assert_eq!(
                    document.fields().unwrap(),
                    retrieved_document.fields().unwrap()
                );
                assert_eq!(document.id(), specific_document.id());
                assert_eq!(
                    document.fields().unwrap(),
                    specific_document.fields().unwrap()
                );
            }
        })
    }

    #[rstest]
    fn gets_documents_by_schema(
        #[from(populate_store_config)]
        #[with(2, 10, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            populate_and_materialize(&mut node, &config).await;

            // Retrieve these documents by their schema id.
            let schema_documents = node
                .context
                .store
                .get_documents_by_schema(config.schema.id())
                .await
                .expect("Get document by schema");

            // There should be ten.
            assert_eq!(schema_documents.len(), 10);
        });
    }

    #[rstest]
    fn prunes_document_view(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document = documents[0].clone();
            let first_document_view_id: DocumentViewId = document.id().as_str().parse().unwrap();

            // Get the current document from the store.
            let current_document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap();

            // Get the current view id.
            let current_document_view_id = current_document.unwrap().view_id().to_owned();

            // Reduce a historic view of an existing document.
            let _ = reduce_task(
                node.context.clone(),
                TaskInput::DocumentViewId(first_document_view_id.clone()),
            )
            .await;

            // Get that view again to check it's in the db.
            let document = node
                .context
                .store
                .get_document_by_view_id(&first_document_view_id)
                .await
                .unwrap();
            assert!(document.is_some());

            // Prune the first document view.
            let result = node
                .context
                .store
                .prune_document_view(&first_document_view_id)
                .await;
            assert!(result.is_ok());
            // Returns `true` when pruning succeeded.
            assert!(result.unwrap());

            // Get the first document view again, it should no longer be there.
            let document = node
                .context
                .store
                .get_document_by_view_id(&first_document_view_id)
                .await
                .unwrap();
            assert!(document.is_none());

            // Get the current view of the document to make sure that wasn't deleted too.
            let document = node
                .context
                .store
                .get_document_by_view_id(&current_document_view_id)
                .await
                .unwrap();
            assert!(document.is_some());
        });
    }

    #[rstest]
    fn does_not_prune_pinned_views(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
        key_pair: KeyPair,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document = documents[0].clone();
            let first_document_view_id: DocumentViewId = document.id().as_str().parse().unwrap();

            // Reduce a historic view of an existing document.
            let _ = reduce_task(
                node.context.clone(),
                TaskInput::DocumentViewId(first_document_view_id.clone()),
            )
            .await;

            // Add a new document to the store which pins the first view of the above document.
            add_schema_and_documents(
                &mut node,
                "new_schema",
                vec![vec![(
                    "pin_document",
                    first_document_view_id.clone().into(),
                    Some(config.schema.id().to_owned()),
                )]],
                &key_pair,
            )
            .await;

            // Attempt to prune the first document view.
            let result = node
                .context
                .store
                .prune_document_view(&first_document_view_id)
                .await;
            assert!(result.is_ok());
            // Returns `false` when pruning failed.
            assert!(!result.unwrap());

            // Get the first document view, it should still be in the store as it was pinned.
            let document = node
                .context
                .store
                .get_document_by_view_id(&first_document_view_id)
                .await
                .unwrap();
            assert!(document.is_some());
        });
    }

    #[rstest]
    fn does_not_prune_current_view(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document = documents[0].clone();

            // Attempt to prune the current document view.
            let result = node
                .context
                .store
                .prune_document_view(document.view_id())
                .await;
            assert!(result.is_ok());
            // Returns `false` when pruning failed.
            assert!(!result.unwrap());

            // Get the current document view, it should still be in the store.
            let document = node
                .context
                .store
                .get_document_by_view_id(document.view_id())
                .await
                .unwrap();
            assert!(document.is_some());
        });
    }

    #[rstest]
    fn purge_document(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document_id = documents[0].id();

            // There is one document in the database which contains an CREATE and UPDATE operation
            // which were both published by the same author. These are the number of rows we
            // expect for each table.
            assert_query(&node, "SELECT entry_hash FROM entries", 2).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 2).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 28).await;
            assert_query(&node, "SELECT log_id FROM logs", 1).await;
            assert_query(&node, "SELECT document_id FROM documents", 1).await;
            assert_query(&node, "SELECT document_id FROM document_views", 1).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 11).await;

            // Purge this document from the database, we now expect all tables to be empty.
            let result = node.context.store.purge_document(document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 0).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 0).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 0).await;
            assert_query(&node, "SELECT log_id FROM logs", 1).await;
            assert_query(&node, "SELECT document_id FROM documents", 0).await;
            assert_query(&node, "SELECT document_id FROM document_views", 0).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 0).await;
        });
    }

    #[rstest]
    fn purging_only_effects_target_document(
        #[from(populate_store_config)]
        #[with(1, 2, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document_id = documents[0].id();

            // There are two documents in the database which each contain a single CREATE operation
            // and they were published by the same author. These are the number of rows we expect
            // for each table.
            assert_query(&node, "SELECT entry_hash FROM entries", 2).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 2).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 28).await;
            assert_query(&node, "SELECT log_id FROM logs", 2).await;
            assert_query(&node, "SELECT document_id FROM documents", 2).await;
            assert_query(&node, "SELECT document_id FROM document_views", 2).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 22).await;

            // Purge one document from the database, we now expect half the rows to be remaining.
            let result = node.context.store.purge_document(document_id).await;
            assert!(result.is_ok(), "{:#?}", result);

            assert_query(&node, "SELECT entry_hash FROM entries", 1).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 1).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 14).await;
            assert_query(&node, "SELECT log_id FROM logs", 2).await;
            assert_query(&node, "SELECT document_id FROM documents", 1).await;
            assert_query(&node, "SELECT document_id FROM document_views", 1).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 11).await;
        });
    }

    #[rstest]
    fn next_args_after_purge(
        #[from(populate_store_config)]
        #[with(2, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document_id = documents[0].id();
            let public_key = config.authors[0].public_key();

            let _ = node.context.store.purge_document(document_id).await;

            let result = next_args(
                &node.context.store,
                &public_key,
                Some(&document_id.as_str().parse().unwrap()),
            )
            .await;

            assert!(result.is_err());

            let result = next_args(&node.context.store, &public_key, None).await;

            assert!(result.is_ok());
            let next_args = result.unwrap();
            assert_eq!(next_args, (None, None, SeqNum::default(), LogId::new(1)));
        });
    }

    #[rstest]
    fn regression_handle_null_relation_list_value(
        #[from(populate_store_config)]
        #[with(1, 1, vec![KeyPair::new()])]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let documents = populate_and_materialize(&mut node, &config).await;
            let document = documents[0].clone();

            // The default test document contains an empty pinned relation list field.
            let result = node
                .context
                .store
                .get_child_document_ids(&document.view_id())
                .await;

            assert!(result.is_ok());
        });
    }
}
