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
//! The whole document store can be seen as a live cache. All it's content is derived from
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
//! The getter methods allow retrieving a document by it's `DocumentId` or it's
//! `DocumentViewId`. The former always returns the most current document state, the latter
//! returns the specific document view if it has already been materialised and stored. Although it
//! is possible to construct a document at any point in it's history if all operations are
//! retained, we use a system of "pinned relations" to identify and materialise only views we
//! explicitly wish to keep.
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

    /// Get a document from the store by it's `DocumentId`.
    ///
    /// Retrieves a document in it's most current state from the store. Ignores documents which
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
    /// Get's a document at a specific point in it's history. Only returns views that have already
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

        // Unwrap as we can assume a document for the found document id exists.
        let document_row = document_row.unwrap();

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
                documents.schema_id = $1  AND documents.is_deleted = false
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
    /// If the document already existed in the store then it's current view and view id will be
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

    /// Iterate over all views of a document and delete any which:
    /// - are not the current view
    /// - _and_ no document field exists in the database which contains a pinned relation to this view
    /// 
    /// Returns the document ids of any document which were related to in a pinned relation of the
    /// deleted views. It's useful to return these documents as they themselves may now be
    /// dangling and require pruning. 
    pub async fn prune_document_views(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<DocumentId>, DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.

        // Collect all views _except_ the current view for this document
        let historic_document_view_ids: Vec<String> = query_scalar(
            "
            SELECT 
                document_views.document_view_id
            FROM 
                document_views
            LEFT JOIN 
                documents 
            ON 
                documents.document_view_id = document_views.document_view_id
            WHERE 
                document_views.document_id = $1
            AND 
                documents.document_view_id IS NULL
            ",
        )
        .bind(document_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        // Iterate over all document views and delete them if no document field exists in the
        // database which contains a pinned relation to this view.
        //
        // Deletes on "document_views" cascade to "document_view_fields" so rows there are also removed
        // from the database.
        let mut effected_child_relations = vec![];

        for document_view_id in &historic_document_view_ids {
            // Before attempting to delete this view we need to fetch the ids of any child documents
            // which would be effected by this view being deleted. This is so if the deletion goes
            // ahead, we can return these values as they are useful for performing further garbage
            // collection tasks.
            let effected_children_ids: Vec<String> = query_scalar(
                "
                SELECT DISTINCT 
                    document_views.document_id
                FROM
                    document_views
                WHERE 
                    document_views.document_view_id 
                IN (
                    SELECT
                        operation_fields_v1.value
                    FROM
                        operation_fields_v1
                    LEFT JOIN 
                        document_view_fields
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                    AND
                        document_view_fields.name = operation_fields_v1.name
                    LEFT JOIN 
                        documents 
                    ON 
                        documents.document_view_id = document_views.document_view_id
                    WHERE
                        operation_fields_v1.field_type IN ('pinned_relation', 'pinned_relation_list')
                    AND 
                        document_view_fields.document_view_id = $1
                    AND 
                        documents.document_view_id IS NULL
                )
                ",
            )
            .bind(document_view_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

            // Attempt to delete the view. If it is pinned from an existing view the deletion will
            // not go ahead.
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
                ",
            )
            .bind(document_view_id)
            .execute(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

            // If any rows were affected (the deletion went ahead) then extend the
            // uneffected_child_relations list with the previously collected ids.
            if result.rows_affected() > 0 {
                debug!("Deleted view: {}", document_view_id);
                effected_child_relations.extend(effected_children_ids);
            } else {
                debug!("Did not delete view: {}", document_view_id);
            }
        }

        let effected_child_relations: Vec<DocumentId> = effected_child_relations
            .iter()
            .map(|document_id| document_id.parse().unwrap())
            .collect();

        Ok(effected_child_relations)
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
    // document view and the operation values which hold it's field values.
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
        LEFT JOIN document_views
            ON
                document_view_fields.document_view_id = document_views.document_view_id
        LEFT JOIN operation_fields_v1
            ON
                document_view_fields.operation_id = operation_fields_v1.operation_id
            AND
                document_view_fields.name = operation_fields_v1.name
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

    // If the document is not deleted, then we also want to insert it's view and fields.
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
    use p2panda_rs::document::materialization::build_graph;
    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentBuilder, DocumentId, DocumentViewFields, DocumentViewId};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::traits::AsOperation;
    use p2panda_rs::operation::{Operation, OperationId};
    use p2panda_rs::storage_provider::traits::{DocumentStore, OperationStore};
    use p2panda_rs::test_utils::constants;
    use p2panda_rs::test_utils::fixtures::{
        key_pair, operation, random_document_id, random_document_view_id, random_operation_id,
    };
    use p2panda_rs::test_utils::memory_store::helpers::{populate_store, PopulateStoreConfig};
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::db::stores::document::DocumentView;
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::test_utils::{
        add_schema_and_documents, build_document, populate_and_materialize, populate_store_config,
        test_runner, update_document, TestNode,
    };

    #[rstest]
    fn insert_and_get_one_document_view(
        #[from(populate_store_config)]
        #[with(2, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations and build the document.
            let operations = node
                .context
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            // Build the document from the operations.
            let document_builder = DocumentBuilder::from(&operations);
            let document = document_builder.build().unwrap();

            // Insert the document into the store
            let result = node.context.store.insert_document(&document).await;
            assert!(result.is_ok());

            // Find the "CREATE" operation and get it's id.
            let create_operation = WithId::<OperationId>::id(
                operations
                    .iter()
                    .find(|operation| operation.is_create())
                    .unwrap(),
            )
            .to_owned();

            // Build the document with just the first operation.
            let document_at_view_1 = document_builder
                .build_to_view_id(Some(create_operation.into()))
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

            // We should be able to retrieve the document at either of it's views now.

            // Here we request the document with it's initial state.
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

            // Here we request it at it's current state.
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

            // If we retrieve the document by it's id, we expect the current state.
            let retrieved_document = node
                .context
                .store
                .get_document(&document_id)
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
            // We try to retrieve a document view by it's id but no view
            // with that id exists.
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
        #[with(1, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Build the document.
            let document = build_document(&node.context.store, &document_id).await;

            // The document is successfully inserted into the database, this
            // relies on the operations already being present and would fail
            // if they were not.
            let result = node.context.store.insert_document(&document).await;
            assert!(result.is_ok());

            // We can retrieve the most recent document view for this document by it's id.
            let retrieved_document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap()
                .unwrap();

            // We can retrieve a specific document view for this document by it's view_id.
            // In this case, that should be the same as the view retrieved above.
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
        #[with(10, 1, 1, true)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations and build the document.
            let document = build_document(&node.context.store, &document_id).await;
            // Get the view id.
            let view_id = document.view_id();

            // As it has been deleted, there should be no view.
            assert!(document.view().is_none());

            // Here we insert the document. This action also sets it's most recent view.
            let result = node.context.store.insert_document(&document).await;
            assert!(result.is_ok());

            // We retrieve the most recent view for this document by it's document id,
            // but as the document is deleted, we should get a none value back.
            let document = node
                .context
                .store
                .get_document(document.id())
                .await
                .unwrap();
            assert!(document.is_none());

            // We also try to retrieve the specific document view by it's view id.
            // This should also return none as it is deleted.
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
        #[with(10, 1, 1, true)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations and build the document.
            let document = build_document(&node.context.store, &document_id).await;

            // Insert the document, this is possible even though it has been deleted.
            let result = node.context.store.insert_document(&document).await;
            assert!(result.is_ok());

            // When we try to retrieve it by schema id we should NOT get it back.
            let document_views = node
                .context
                .store
                .get_documents_by_schema(constants::schema().id())
                .await
                .unwrap();
            assert!(document_views.is_empty());
        });
    }

    #[rstest]
    fn updates_a_document(
        #[from(populate_store_config)]
        #[with(10, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|node: TestNode| async move {
            // Populate the store with some entries and operations but DON'T materialise any resulting documents.
            let (_, document_ids) = populate_store(&node.context.store, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            // Get the operations for this document and sort them into linear order.
            let operations = node
                .context
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();
            let document_builder = DocumentBuilder::from(&operations);
            let sorted_operations = build_graph(&document_builder.operations())
                .unwrap()
                .sort()
                .unwrap()
                .sorted();

            // We want to test that a document is updated.
            let mut current_operations = Vec::new();
            for operation in sorted_operations {
                // For each operation in the db we insert a document, cumulatively adding the next
                // operation each time. this should perform an "INSERT" first in the documents
                // table, followed by 9 "UPDATES".
                current_operations.push(operation.clone());

                // We build each document.
                let document = DocumentBuilder::new(current_operations.clone())
                    .build()
                    .expect("Build document");

                // Insert it to the database, this should also update it's view.
                node.context
                    .store
                    .insert_document(&document)
                    .await
                    .expect("Insert document");

                // We can retrieve the document by it's document id.
                let retrieved_document = node
                    .context
                    .store
                    .get_document(document.id())
                    .await
                    .expect("Get document")
                    .expect("Unwrap document");

                // And also directly by it's document view id.
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
        #[with(2, 10, 1)]
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
    fn prunes_document_views(
        #[from(populate_store_config)]
        #[with(2, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let (_, document_ids) = populate_and_materialize(&mut node, &config).await;
            let document_id = document_ids[0].clone();
            let first_document_view_id: DocumentViewId = document_id.as_str().parse().unwrap();

            // Get the current document from the store.
            let current_document = node.context.store.get_document(&document_id).await.unwrap();

            println!("{current_document:#?}");

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

            // Now prune dangling views for the document.
            let result = node.context.store.prune_document_views(&document_id).await;
            assert!(result.is_ok());
            // This should be `0` because even though the default test document contains pinned
            // relations none of them have been materialized into the store, so there actually are
            // zero effected children for this pruning.
            assert_eq!(result.unwrap().len(), 0);

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
        #[with(2, 1, 1)]
        config: PopulateStoreConfig,
        key_pair: KeyPair,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            let (_, document_ids) = populate_and_materialize(&mut node, &config).await;
            let document_id = document_ids[0].clone();
            let first_document_view_id: DocumentViewId = document_id.as_str().parse().unwrap();

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

            // Now prune dangling views for the document.
            let result = node.context.store.prune_document_views(&document_id).await;
            assert!(result.is_ok());

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
    fn recursive_pruning(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish some documents which we will later point relations at.
            let (child_schema, child_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_child",
                vec![
                    vec![("uninteresting_field", 1.into(), None)],
                    vec![("uninteresting_field", 2.into(), None)],
                ],
                &key_pair,
            )
            .await;

            // Create some parent documents which contain a pinned relation list pointing to the
            // children created above.
            let (parent_schema, parent_document_view_ids) = add_schema_and_documents(
                &mut node,
                "schema_for_parent",
                vec![vec![
                    ("name", "parent".into(), None),
                    (
                        "children",
                        child_document_view_ids.clone().into(),
                        Some(child_schema.id().to_owned()),
                    ),
                ]],
                &key_pair,
            )
            .await;

            // Convert view id to document id.
            let parent_document_id: DocumentId = parent_document_view_ids[0]
                .clone()
                .to_string()
                .parse()
                .unwrap();

            // Update the parent document so that there are now two views stored in the db, one
            // current and one dangling.
            let updated_parent_view_id = update_document(
                &mut node,
                parent_schema.id(),
                vec![("name", "Parent".into())],
                &parent_document_view_ids[0],
                &key_pair,
            )
            .await;

            // Get the historic (dangling) view to check it's actually there.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It is there...
            assert!(historic_document_view.is_some());

            // Create another document, which has a pinned relation to the parent document created
            // above. Now the relation graph looks like this
            //
            // GrandParent --> Parent --> Child1
            //                      \
            //                        --> Child2
            //
            let (schema_for_grand_parent, grand_parent_document_view_ids) =
                add_schema_and_documents(
                    &mut node,
                    "schema_for_grand_parent",
                    vec![vec![
                        ("name", "grand parent".into(), None),
                        (
                            "child",
                            parent_document_view_ids[0].clone().into(),
                            Some(parent_schema.id().to_owned()),
                        ),
                    ]],
                    &key_pair,
                )
                .await;

            // Convert view id to document id.
            let grand_parent_document_id: DocumentId = grand_parent_document_view_ids[0]
                .clone()
                .to_string()
                .parse()
                .unwrap();

            // Update the grand parent document to a new view, leaving the previous one dangling.
            //
            // Note: this method _does not_ dispatch "prune" tasks.
            update_document(
                &mut node,
                schema_for_grand_parent.id(),
                vec![
                    ("name", "Grand Parent".into()),
                    ("child", updated_parent_view_id.into()),
                ],
                &grand_parent_document_view_ids[0],
                &key_pair,
            )
            .await;

            // Get the historic (dangling) view to make sure it exists.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&grand_parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It does...
            assert!(historic_document_view.is_some());

            // Now prune dangling views for the grand parent document. This method deletes any
            // dangling views (not pinned, not current) from the database for this document. It
            // returns the document ids of any documents which may have views which have become
            // "un-pinned" as a result of this view being removed. In this case, that's the
            // document id of the "parent" document.
            let result = node
                .context
                .store
                .prune_document_views(&grand_parent_document_id)
                .await;

            assert!(result.is_ok());
            let effected_document_ids = result.unwrap();
            // One effected document id is returned.
            assert_eq!(effected_document_ids.len(), 1);
            // It is the parent (which this grand parent relates to) as we expect.
            assert_eq!(effected_document_ids[0], parent_document_id);

            // Check the historic view has been deleted.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&grand_parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It has...
            assert!(historic_document_view.is_none());

            // Now prune dangling views for the parent document.
            let result = node
                .context
                .store
                .prune_document_views(&effected_document_ids[0])
                .await;

            assert!(result.is_ok());
            let effected_document_ids = result.unwrap();
            // We expect this to be 0 as the only potentially effected views for this document are
            // current views for their document, which we know won't be garbage collected.
            assert_eq!(effected_document_ids.len(), 0);

            // Check the historic view has been deleted.
            let historic_document_view = node
                .context
                .store
                .get_document_by_view_id(&parent_document_view_ids[0].clone())
                .await
                .unwrap();

            // It has.
            assert!(historic_document_view.is_none());
        });
    }
}
