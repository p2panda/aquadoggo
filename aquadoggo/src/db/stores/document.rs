// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

use futures::future::try_join_all;
use p2panda_rs::document::{Document, DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;
use sqlx::{query, query_as};

use crate::db::models::DocumentViewFieldRow;
use crate::db::sql_store::SqlStore;
use crate::db::utils::parse_document_view_field_rows;

impl DocumentStore for SqlStore {}

impl SqlStore {
    /// Insert a document_view into the db.
    ///
    /// Internally, this method performs two different operations:
    /// - insert a row for every document_view_field present on this view
    /// - insert a row for the document_view itself
    ///
    /// If either of these operations fail and error is returned.
    pub async fn insert_document_view(
        &self,
        document_view: &DocumentView,
        schema_id: &SchemaId,
    ) -> Result<(), DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Insert document view field relations into the db
        let field_relations_insertion_result =
            try_join_all(document_view.iter().map(|(name, value)| {
                query(
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
                .execute(&self.pool)
            }))
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Insert document view into the db
        let document_view_insertion_result = query(
            "
            INSERT INTO
                document_views (
                    document_view_id,
                    schema_id
                )
            VALUES
                ($1, $2)
            ",
        )
        .bind(document_view.id().to_string())
        .bind(schema_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Check every insertion performed affected exactly 1 row.
        if document_view_insertion_result.rows_affected() != 1
            || field_relations_insertion_result
                .iter()
                .any(|query_result| query_result.rows_affected() != 1)
        {
            return Err(DocumentStorageError::DocumentViewInsertionError(
                document_view.id().clone(),
            ));
        }

        // Commit the transaction.
        transaction
            .commit()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        Ok(())
    }

    /// Get a document view from the database by it's id.
    ///
    /// Internally, this method retrieve all document rows related to this document view id
    /// and then from these constructs the document view itself.
    ///
    /// An error is returned if any of the above steps fail or a fatal database error occured.
    pub async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<Option<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                document_view_fields.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
                operation_fields_v1.list_index,
                operation_fields_v1.field_type,
                operation_fields_v1.value
            FROM
                document_view_fields
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = document_view_fields.operation_id
                AND
                    operation_fields_v1.name = document_view_fields.name
            WHERE
                document_view_fields.document_view_id = $1
            ORDER BY
                operation_fields_v1.list_index ASC
            ",
        )
        .bind(id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        let view = if document_view_field_rows.is_empty() {
            None
        } else {
            Some(DocumentView::new(
                id,
                &parse_document_view_field_rows(document_view_field_rows),
            ))
        };

        Ok(view)
    }

    /// Insert a document and it's latest document view into the database.
    ///
    /// This method inserts or updates a row into the documents table and then makes a call
    /// to `insert_document_view()` to insert the new document view for this document.
    ///
    /// Note: "out-of-date" document views will remain in storage when a document already
    /// existed and is updated. If they are not needed for anything else they can be garbage
    /// collected.
    pub async fn insert_document(&self, document: &Document) -> Result<(), DocumentStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // Insert document view into the db
        let document_insertion_result = query(
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
        .bind(document.schema().to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        if document_insertion_result.rows_affected() != 1 {
            return Err(DocumentStorageError::DocumentInsertionError(
                document.id().clone(),
            ));
        }

        if !document.is_deleted() && document.view().is_some() {
            let document_view =
                DocumentView::new(document.view_id(), document.view().unwrap().fields());

            self.insert_document_view(&document_view, document.schema())
                .await?;
        };

        // Commit the transaction.
        transaction
            .commit()
            .await
            .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        Ok(())
    }

    /// Get a documents' latest document view from the database by it's `DocumentId`.
    ///
    /// Retrieve the current document view for a specified document. If the document
    /// has been deleted then None is returned. An error is returned is a fatal database
    /// error occurs.
    pub async fn get_latest_document_view(
        &self,
        id: &DocumentId,
    ) -> Result<Option<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                document_view_fields.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
                operation_fields_v1.list_index,
                operation_fields_v1.field_type,
                operation_fields_v1.value
            FROM
                documents
            LEFT JOIN document_view_fields
                ON
                    documents.document_view_id = document_view_fields.document_view_id
            LEFT JOIN operation_fields_v1
                ON
                    document_view_fields.operation_id = operation_fields_v1.operation_id
                AND
                    document_view_fields.name = operation_fields_v1.name
            WHERE
                documents.document_id = $1 AND documents.is_deleted = false
            ORDER BY
                operation_fields_v1.list_index ASC
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        if document_view_field_rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(DocumentView::new(
            &document_view_field_rows[0]
                .document_view_id
                .parse()
                .unwrap(),
            &parse_document_view_field_rows(document_view_field_rows),
        )))
    }

    /// Get all documents which follow the passed schema id from the database
    ///
    /// Retrieve the latest document view for all documents which follow the specified schema.
    ///
    /// An error is returned is a fatal database error occurs.
    pub async fn get_latest_document_views_by_schema(
        &self,
        schema_id: &SchemaId,
    ) -> Result<Vec<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                documents.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
                operation_fields_v1.list_index,
                operation_fields_v1.field_type,
                operation_fields_v1.value
            FROM
                documents
            LEFT JOIN document_view_fields
                ON
                    documents.document_view_id = document_view_fields.document_view_id
            LEFT JOIN operation_fields_v1
                ON
                    document_view_fields.operation_id = operation_fields_v1.operation_id
                AND
                    document_view_fields.name = operation_fields_v1.name
            WHERE
                documents.schema_id = $1 AND documents.is_deleted = false
            ORDER BY
                operation_fields_v1.list_index ASC
            ",
        )
        .bind(schema_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        // We need to group all returned field rows by their document_view_id so we can then
        // build schema from them.
        let mut grouped_document_field_rows: BTreeMap<String, Vec<DocumentViewFieldRow>> =
            BTreeMap::new();

        for row in document_view_field_rows {
            let existing_view = grouped_document_field_rows.get_mut(&row.document_view_id);
            if let Some(existing_view) = existing_view {
                existing_view.push(row)
            } else {
                grouped_document_field_rows.insert(row.clone().document_view_id, vec![row]);
            };
        }

        let document_views: Vec<DocumentView> = grouped_document_field_rows
            .iter()
            .map(|(id, document_field_row)| {
                let fields = parse_document_view_field_rows(document_field_row.to_owned());
                DocumentView::new(&id.parse().unwrap(), &fields)
            })
            .collect();

        Ok(document_views)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentBuilder, DocumentViewFields, DocumentViewId};
    use p2panda_rs::operation::traits::AsOperation;
    use p2panda_rs::operation::{Operation, OperationId};
    use p2panda_rs::test_utils::constants::{self};
    use p2panda_rs::test_utils::fixtures::{
        operation, random_document_view_id, random_operation_id,
    };
    use rstest::rstest;

    use crate::db::stores::document::DocumentView;
    use crate::db::stores::test_utils::{
        build_document, doggo_schema, test_db, TestDatabase, TestDatabaseRunner,
    };

    #[rstest]
    fn insert_and_get_one_document_view(
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Operations for this document id exist in the database.
            let document_id = db.test_data.documents[0].clone();

            // Get the operations and build the document.
            let document = build_document(&db.store, &document_id).await;

            // Get it's document view and insert it in the database.
            let document_view = document.view().expect("Get document view");

            // Insert the view into the store.
            let result = db
                .store
                .insert_document_view(document_view, document.schema())
                .await;
            assert!(result.is_ok());

            // We should be able to retrieve the document view now by it's view_id.
            let retrieved_document_view = db
                .store
                .get_document_view_by_id(document_view.id())
                .await
                .unwrap()
                .unwrap();

            // The retrieved view should the expected fields.
            assert_eq!(retrieved_document_view.len(), 9);

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
                assert!(retrieved_document_view.get(key).is_some());
                assert_eq!(retrieved_document_view.get(key), document_view.get(key));
            }
        });
    }

    #[rstest]
    fn document_view_does_not_exist(
        random_document_view_id: DocumentViewId,
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // We try to retrieve a document view by it's id but no view
            // with that id exists.
            let view_does_not_exist = db
                .store
                .get_document_view_by_id(&random_document_view_id)
                .await
                .unwrap();

            // The return result should contain a none value.
            assert!(view_does_not_exist.is_none());
        });
    }

    #[rstest]
    fn insert_document_view_with_missing_operation(
        #[from(random_operation_id)] operation_id: OperationId,
        #[from(random_document_view_id)] document_view_id: DocumentViewId,
        #[from(test_db)] runner: TestDatabaseRunner,
        operation: Operation,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
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
            let result = db
                .store
                .insert_document_view(&document_view, constants::schema().id())
                .await;

            assert!(result.is_err());
        });
    }

    #[rstest]
    fn inserts_gets_document(
        #[from(test_db)]
        #[with(1, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Operations for this document id exist in the database.
            let document_id = db.test_data.documents[0].clone();
            // Build the document and view.
            let document = build_document(&db.store, &document_id).await;
            let expected_document_view = document.view().expect("Get document view");

            // The document is successfully inserted into the database, this
            // relies on the operations already being present and would fail
            // if they were not.
            let result = db.store.insert_document(&document).await;
            assert!(result.is_ok());

            // We can retrieve the most recent document view for this document by it's id.
            let most_recent_document_view = db
                .store
                .get_latest_document_view(document.id())
                .await
                .unwrap()
                .unwrap();

            // We can retrieve a specific document view for this document by it's view_id.
            // In this case, that should be the same as the view retrieved above.
            let specific_document_view = db
                .store
                .get_document_view_by_id(document.view_id())
                .await
                .unwrap()
                .unwrap();

            // The retrieved views should both have 9 fields.
            assert_eq!(most_recent_document_view.len(), 9);
            assert_eq!(specific_document_view.len(), 9);

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
                assert!(most_recent_document_view.get(key).is_some());
                assert_eq!(
                    most_recent_document_view.get(key),
                    expected_document_view.get(key)
                );
                assert!(specific_document_view.get(key).is_some());
                assert_eq!(
                    specific_document_view.get(key),
                    expected_document_view.get(key)
                );
            }
        });
    }

    #[rstest]
    fn no_view_when_document_deleted(
        #[from(test_db)]
        #[with(10, 1, 1, true)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Operations for this document id exist in the database.
            let document_id = db.test_data.documents[0].clone();

            // Get the operations and build the document.
            let document = build_document(&db.store, &document_id).await;
            // Get the view id.
            let view_id = document.view_id();

            // As it has been deleted, there should be no view.
            assert!(document.view().is_none());

            // Here we insert the document. This action also sets it's most recent view.
            let result = db.store.insert_document(&document).await;
            assert!(result.is_ok());

            // We retrieve the most recent view for this document by it's document id,
            // but as the document is deleted, we should get a none value back.
            let document_view = db
                .store
                .get_latest_document_view(document.id())
                .await
                .unwrap();
            assert!(document_view.is_none());

            // We also try to retrieve the specific document view by it's view id.
            // This should also return none as it is deleted.
            let document_view = db.store.get_document_view_by_id(view_id).await.unwrap();
            assert!(document_view.is_none());
        });
    }

    #[rstest]
    fn get_documents_by_schema_deleted_document(
        #[from(test_db)]
        #[with(10, 1, 1, true)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Operations for this document id exist in the database.
            let document_id = db.test_data.documents[0].clone();

            // Get the operations and build the document.
            let document = build_document(&db.store, &document_id).await;

            let result = db.store.insert_document(&document).await;
            assert!(result.is_ok());

            let document_views = db
                .store
                .get_latest_document_views_by_schema(constants::schema().id())
                .await
                .unwrap();
            assert!(document_views.is_empty());
        });
    }

    #[rstest]
    fn updates_a_document(
        #[from(test_db)]
        #[with(10, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Operations for this document id exist in the database.
            let document_id = db.test_data.documents[0].clone();

            // Get the operations and build the document.
            let document = build_document(&db.store, &document_id).await;
            // Get the oredered operations.
            let sorted_operations = document.operations();

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
                db.store
                    .insert_document(&document)
                    .await
                    .expect("Insert document");

                // We can retrieve the document's latest view by it's document id.
                let latest_document_view = db
                    .store
                    .get_latest_document_view(document.id())
                    .await
                    .expect("Get document view");

                // And also retrieve the latest document view directly by it's document view id.
                let specific_document_view = db
                    .store
                    .get_document_view_by_id(document.view_id())
                    .await
                    .expect("Get document view");

                // The views should equal the current view of the document we inserted.
                // This includes the value and the view id.
                assert_eq!(document.view(), latest_document_view.as_ref());
                assert_eq!(document.view(), specific_document_view.as_ref());
            }
        })
    }

    #[rstest]
    fn gets_documents_by_schema(
        #[from(test_db)]
        #[with(2, 10, 1, false)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Insert two documents which have the same schema.
            for document_id in &db.test_data.documents {
                // Get the operations and build the document.
                let document = build_document(&db.store, document_id).await;
                db.store
                    .insert_document(&document)
                    .await
                    .expect("Insert document");
            }

            // Retrieve these documents by their schema id.
            let schema_documents = db
                .store
                .get_latest_document_views_by_schema(doggo_schema().id())
                .await
                .expect("Get document by schema");

            // There should be two.
            assert_eq!(schema_documents.len(), 10);
        });
    }
}
