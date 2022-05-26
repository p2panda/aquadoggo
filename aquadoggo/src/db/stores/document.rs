// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::{Document, DocumentId, DocumentView, DocumentViewId};
use p2panda_rs::schema::SchemaId;
use sqlx::{query, query_as};

use crate::db::errors::DocumentStorageError;
use crate::db::models::document::DocumentViewFieldRow;
use crate::db::provider::SqlStorage;
use crate::db::traits::DocumentStore;
use crate::db::utils::parse_document_view_field_rows;

#[async_trait]
impl DocumentStore for SqlStorage {
    /// Insert a document_view into the db.
    ///
    /// Internally, this method performs two different operations:
    /// - insert a row for every document_view_field present on this view
    /// - insert a row for the document_view itself
    ///
    /// If either of these operations fail and error is returned.
    async fn insert_document_view(
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
                .bind(document_view.id().as_str())
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
        .bind(document_view.id().as_str())
        .bind(schema_id.as_str())
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
    async fn get_document_view_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<Option<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                document_view_fields.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
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
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        let view = if document_view_field_rows.is_empty() {
            None
        } else {
            Some(DocumentView::new(
                &id.to_owned(),
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
    async fn insert_document(&self, document: &Document) -> Result<(), DocumentStorageError> {
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
                    ",
        )
        .bind(document.id().as_str())
        .bind(document.view_id().as_str())
        .bind(document.is_deleted())
        .bind(document.schema().as_str())
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

        Ok(())
    }

    /// Get a documents' latest document view from the database by it's `DocumentId`.
    ///
    /// Retrieve the current document view for a specified document. If the document
    /// has been deleted then None is returned. An error is returned is a fatal database
    /// error occurs.
    async fn get_document_by_id(
        &self,
        id: &DocumentId,
    ) -> Result<Option<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
            SELECT
                document_view_fields.document_view_id,
                document_view_fields.operation_id,
                document_view_fields.name,
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
    async fn get_documents_by_schema(
        &self,
        schema_id: &SchemaId,
    ) -> Result<Vec<DocumentView>, DocumentStorageError> {
        let document_view_field_rows = query_as::<_, DocumentViewFieldRow>(
            "
                SELECT
                    document_view_fields.document_view_id,
                    document_view_fields.operation_id,
                    document_view_fields.name,
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
                    documents.schema_id = $1
                ",
        )
        .bind(schema_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        let mut grouped_document_field_rows: BTreeMap<String, Vec<DocumentViewFieldRow>> =
            BTreeMap::new();

        for document_field_row in document_view_field_rows {
            if let Some(current_operations) =
                grouped_document_field_rows.get_mut(&document_field_row.document_view_id)
            {
                current_operations.push(document_field_row)
            } else {
                grouped_document_field_rows.insert(
                    document_field_row.clone().document_view_id,
                    vec![document_field_row],
                );
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
    use std::convert::TryFrom;
    use std::str::FromStr;

    use p2panda_rs::document::{
        DocumentBuilder, DocumentViewFields, DocumentViewId, DocumentViewValue,
    };
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::Author;
    use p2panda_rs::operation::{AsOperation, OperationId, OperationValue};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, OperationStore};
    use p2panda_rs::test_utils::constants::TEST_SCHEMA_ID;

    use crate::db::stores::document::{DocumentStore, DocumentView};
    use crate::db::stores::entry::StorageEntry;
    use crate::db::stores::test_utils::{test_create_operation, test_db};

    fn entries_to_document_views(entries: &[StorageEntry]) -> Vec<DocumentView> {
        let mut document_views = Vec::new();
        let mut current_document_view_fields = DocumentViewFields::new();
        for entry in entries {
            let operation_id: OperationId = entry.hash().into();
            for (name, value) in entry.operation().fields().unwrap().iter() {
                if entry.operation().is_delete() {
                    continue;
                } else {
                    current_document_view_fields
                        .insert(name, DocumentViewValue::new(&operation_id, value));
                }
            }
            let document_view_fields = DocumentViewFields::new_from_operation_fields(
                &operation_id,
                &entry.operation().fields().unwrap(),
            );
            let document_view =
                DocumentView::new(&operation_id.clone().into(), &document_view_fields);
            document_views.push(document_view)
        }
        document_views
    }

    #[tokio::test]
    async fn inserts_gets_one_document_view() {
        let (storage_provider, key_pairs, _documents) = test_db(1, 1, false).await;
        let key_pair = key_pairs.get(0).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        // Get one entry from the pre-polulated db
        let entry = storage_provider
            .get_entry_at_seq_num(&author, &LogId::new(1), &SeqNum::new(1).unwrap())
            .await
            .unwrap()
            .unwrap();

        // Construct a `DocumentView`
        let operation_id: OperationId = entry.hash().into();
        let document_view_id: DocumentViewId = operation_id.clone().into();
        let document_view = DocumentView::new(
            &document_view_id,
            &DocumentViewFields::new_from_operation_fields(
                &operation_id,
                &entry.operation().fields().unwrap(),
            ),
        );

        // Insert into db
        let result = storage_provider
            .insert_document_view(&document_view, &SchemaId::from_str(TEST_SCHEMA_ID).unwrap())
            .await;

        assert!(result.is_ok());

        let retrieved_document_view = storage_provider
            .get_document_view_by_id(&document_view_id)
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
        ] {
            assert!(retrieved_document_view.get(key).is_some());
            assert_eq!(retrieved_document_view.get(key), document_view.get(key));
        }
    }

    #[tokio::test]
    async fn document_view_does_not_exist() {
        let (storage_provider, _key_pairs, _documents) = test_db(1, 1, false).await;

        let view_does_not_exist = storage_provider
            .get_document_view_by_id(&Hash::new_from_bytes(vec![1, 2, 3]).unwrap().into())
            .await
            .unwrap();

        assert!(view_does_not_exist.is_none())
    }

    #[tokio::test]
    async fn inserts_gets_many_document_views() {
        let (storage_provider, key_pairs, _documents) = test_db(10, 1, false).await;
        let key_pair = key_pairs.get(0).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        let log_id = LogId::default();
        let seq_num = SeqNum::default();

        // Get 10 entries from the pre-populated test db
        let entries = storage_provider
            .get_paginated_log_entries(&author, &log_id, &seq_num, 10)
            .await
            .unwrap();

        // Parse them into document views
        let document_views = entries_to_document_views(&entries);

        // Insert each of these views into the db
        for document_view in document_views.clone() {
            storage_provider
                .insert_document_view(&document_view, &schema_id)
                .await
                .unwrap();
        }

        // Retrieve them again and assert they are the same as the inserted ones
        for (count, entry) in entries.iter().enumerate() {
            let result = storage_provider
                .get_document_view_by_id(&entry.hash().into())
                .await;

            assert!(result.is_ok());

            let document_view = result.unwrap().unwrap();

            // The update operation should be included in the view correctly, we check that here.
            let expected_username = if count == 0 {
                DocumentViewValue::new(
                    &entry.hash().into(),
                    &OperationValue::Text("bubu".to_string()),
                )
            } else {
                DocumentViewValue::new(
                    &entry.hash().into(),
                    &OperationValue::Text("yoyo".to_string()),
                )
            };
            assert_eq!(document_view.get("username").unwrap(), &expected_username);
        }
    }

    #[tokio::test]
    async fn insert_document_view_with_missing_operation() {
        let (storage_provider, _key_pairs, documents) = test_db(1, 1, false).await;

        let document_id = documents.get(0).unwrap();
        let operation_id: OperationId = Hash::new_from_bytes(vec![0, 1, 2]).unwrap().into();

        let document_view = DocumentView::new(
            &document_id.as_str().parse().unwrap(),
            &DocumentViewFields::new_from_operation_fields(
                &operation_id,
                &test_create_operation().fields().unwrap(),
            ),
        );

        let result = storage_provider
            .insert_document_view(&document_view, &SchemaId::from_str(TEST_SCHEMA_ID).unwrap())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "A fatal error occured in DocumentStore: error returned from database: FOREIGN KEY constraint failed".to_string()
        );
    }

    #[tokio::test]
    async fn inserts_gets_document() {
        let (storage_provider, _key_pairs, documents) = test_db(1, 1, false).await;

        let document_id = documents.get(0).unwrap();

        let document_operations = storage_provider
            .get_operations_by_document_id(document_id)
            .await
            .unwrap();

        // We're accessing the wrapped `Operation` here, I think there could be a nicer pattern for this, see: https://github.com/p2panda/p2panda/issues/320
        let document = DocumentBuilder::new(
            document_operations
                .into_iter()
                .map(|operation| operation.into())
                .collect(),
        )
        .build()
        .unwrap();

        let result = storage_provider.insert_document(&document).await;

        assert!(result.is_ok());

        let document_view = storage_provider
            .get_document_view_by_id(document.view_id())
            .await
            .unwrap()
            .unwrap();

        let expected_document_view = document.view().unwrap();

        for key in [
            "username",
            "age",
            "height",
            "is_admin",
            "profile_picture",
            "many_profile_pictures",
            "special_profile_picture",
            "many_special_profile_pictures",
        ] {
            assert!(document_view.get(key).is_some());
            assert_eq!(document_view.get(key), expected_document_view.get(key));
        }
    }

    #[tokio::test]
    async fn gets_document_by_id() {
        let (storage_provider, _key_pairs, documents) = test_db(1, 1, false).await;

        let document_id = documents.get(0).unwrap();

        let document_operations = storage_provider
            .get_operations_by_document_id(document_id)
            .await
            .unwrap();

        // We're accessing the wrapped `Operation` here, I think there could be a nicer pattern for this, see: https://github.com/p2panda/p2panda/issues/320
        let document = DocumentBuilder::new(
            document_operations
                .into_iter()
                .map(|operation| operation.into())
                .collect(),
        )
        .build()
        .unwrap();

        let result = storage_provider.insert_document(&document).await;

        assert!(result.is_ok());

        let document_view = storage_provider
            .get_document_by_id(document.id())
            .await
            .unwrap()
            .unwrap();

        let expected_document_view = document.view().unwrap();

        for key in [
            "username",
            "age",
            "height",
            "is_admin",
            "profile_picture",
            "many_profile_pictures",
            "special_profile_picture",
            "many_special_profile_pictures",
        ] {
            assert!(document_view.get(key).is_some());
            assert_eq!(document_view.get(key), expected_document_view.get(key));
        }
    }

    #[tokio::test]
    async fn no_view_when_document_deleted() {
        let (storage_provider, _key_pairs, documents) = test_db(3, 1, true).await;

        let document_id = documents.get(0).unwrap();

        let document_operations = storage_provider
            .get_operations_by_document_id(document_id)
            .await
            .unwrap();

        // We're accessing the wrapped `Operation` here, I think there could be a nicer pattern for this, see: https://github.com/p2panda/p2panda/issues/320
        let document = DocumentBuilder::new(
            document_operations
                .into_iter()
                .map(|operation| operation.into())
                .collect(),
        )
        .build()
        .unwrap();

        let result = storage_provider.insert_document(&document).await;

        assert!(result.is_ok());

        let document_view = storage_provider
            .get_document_by_id(document.id())
            .await
            .unwrap();

        assert!(document_view.is_none());
    }

    #[tokio::test]
    async fn gets_documents_by_schema() {
        let (storage_provider, _key_pairs, documents) = test_db(1, 2, false).await;
        let schema_id = SchemaId::from_str(TEST_SCHEMA_ID).unwrap();

        for document_id in documents {
            let document_operations = storage_provider
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            let document = DocumentBuilder::new(
                document_operations
                    .into_iter()
                    .map(|operation| operation.into())
                    .collect(),
            )
            .build()
            .unwrap();

            storage_provider.insert_document(&document).await.unwrap();
        }

        let schema_documents = storage_provider
            .get_documents_by_schema(&schema_id)
            .await
            .unwrap();

        assert_eq!(schema_documents.len(), 2)
    }
}
