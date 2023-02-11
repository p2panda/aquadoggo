// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::traits::AsOperation;
use p2panda_rs::operation::{Operation, OperationId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::OperationStorageError;
use p2panda_rs::storage_provider::traits::OperationStore;
use sqlx::{query, query_as, query_scalar};

use crate::db::models::utils::{parse_operation_rows, parse_value_to_string_vec};
use crate::db::models::OperationFieldsJoinedRow;
use crate::db::types::StorageOperation;
use crate::db::SqlStore;

/// Implementation of `OperationStore` trait which is required when constructing a
/// `StorageProvider`.
///
/// Handles storage and retrieval of operations in the form of `VerifiedOperation` which implements
/// the required `AsVerifiedOperation` trait.
///
/// There are several intermediary structs defined in `db/models/` which represent rows from tables
/// in the database where this entry, it's fields and opreation relations are stored. These are
/// used in conjunction with the `sqlx` library to coerce raw values into structs when querying the
/// database.
#[async_trait]
impl OperationStore for SqlStore {
    type Operation = StorageOperation;

    /// Get the id of the document an operation is part of.
    ///
    /// Returns a result containing a `DocumentId` wrapped in an option. If no document was found,
    /// then this method returns None. Errors if a fatal storage error occurs.
    async fn get_document_id_by_operation_id(
        &self,
        id: &OperationId,
    ) -> Result<Option<DocumentId>, OperationStorageError> {
        let document_id: Option<String> = query_scalar(
            "
            SELECT
                document_id
            FROM
                operations_v1
            WHERE
                operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        Ok(document_id.map(|id_str| id_str.parse().unwrap()))
    }

    /// Insert an operation into storage.
    ///
    /// This requires a DoggoOperation to be composed elsewhere, it contains an `PublicKey`,
    /// `DocumentId`, `OperationId` and the actual `Operation` we want to store.
    ///
    /// Returns a result containing `true` when one insertion occured, and false when no insertions
    /// occured. Errors when a fatal storage error occurs.
    ///
    /// In aquadoggo we store an operation in the database in three different tables: `operations`,
    /// `previous` and `operation_fields`. This means that this method actually makes 3
    /// different sets of insertions.
    async fn insert_operation(
        &self,
        id: &OperationId,
        public_key: &PublicKey,
        operation: &Operation,
        document_id: &DocumentId,
    ) -> Result<(), OperationStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()` will
        // be rolled back in the event of an error.
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Construct query for inserting operation an row, execute it and check exactly one row was
        // affected.
        let operation_insertion_result = query(
            "
            INSERT INTO
                operations_v1 (
                    public_key,
                    document_id,
                    operation_id,
                    action,
                    schema_id,
                    previous
                )
            VALUES
                ($1, $2, $3, $4, $5, $6)
            ",
        )
        .bind(public_key.to_string())
        .bind(document_id.as_str())
        .bind(id.as_str())
        .bind(operation.action().as_str())
        .bind(operation.schema_id().to_string())
        .bind(
            operation
                .previous()
                .map(|document_view_id| document_view_id.to_string()),
        )
        .execute(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Construct and execute the queries, return their futures and execute all of them with
        // `try_join_all()`.
        let fields_insertion_result = match operation.fields() {
            Some(fields) => {
                let result = try_join_all(fields.iter().flat_map(|(name, value)| {
                    // If the value is a relation_list or pinned_relation_list we need to insert a
                    // new field row for every item in the list. Here we collect these items and
                    // return them in a vector. If this operation value is anything except for the
                    // above list types, we will return a vec containing a single item.
                    let db_values = parse_value_to_string_vec(value);

                    // Collect all query futures.
                    db_values
                        .into_iter()
                        .enumerate()
                        .map(|(index, db_value)| {
                            // Compose the query and return it's future.
                            query(
                                "
                            INSERT INTO
                                operation_fields_v1 (
                                    operation_id,
                                    name,
                                    field_type,
                                    value,
                                    list_index
                                )
                            VALUES
                                ($1, $2, $3, $4, $5)
                            ",
                            )
                            .bind(id.as_str().to_owned())
                            .bind(name.to_owned())
                            .bind(value.field_type().to_string())
                            .bind(db_value)
                            .bind(index as i32)
                            .execute(&self.pool)
                        })
                        .collect::<Vec<_>>()
                }))
                .await
                // If any queries error, we catch that here.
                .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

                Some(result)
            }
            None => None,
        };

        // Check every insertion performed affected exactly 1 row.
        if operation_insertion_result.rows_affected() != 1
            || fields_insertion_result
                .unwrap_or_default()
                .iter()
                .any(|query_result| query_result.rows_affected() != 1)
        {
            return Err(OperationStorageError::InsertionError(id.clone()));
        }

        // Commit the transaction.
        transaction
            .commit()
            .await
            .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        Ok(())
    }

    /// Get an operation identified by it's `OperationId`.
    ///
    /// Returns a result containing an `VerifiedOperation` wrapped in an option, if no operation
    /// with this id was found, returns none. Errors if a fatal storage error occured.
    async fn get_operation(
        &self,
        id: &OperationId,
    ) -> Result<Option<StorageOperation>, OperationStorageError> {
        let operation_rows = query_as::<_, OperationFieldsJoinedRow>(
            "
            SELECT
                operations_v1.public_key,
                operations_v1.document_id,
                operations_v1.operation_id,
                operations_v1.action,
                operations_v1.schema_id,
                operations_v1.previous,
                operation_fields_v1.name,
                operation_fields_v1.field_type,
                operation_fields_v1.value,
                operation_fields_v1.list_index
            FROM
                operations_v1
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = operations_v1.operation_id
            WHERE
                operations_v1.operation_id = $1
            ORDER BY
                operation_fields_v1.list_index ASC
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        let operation = parse_operation_rows(operation_rows);
        Ok(operation)
    }

    /// Get all operations that are part of a given document.
    async fn get_operations_by_document_id(
        &self,
        id: &DocumentId,
    ) -> Result<Vec<StorageOperation>, OperationStorageError> {
        let operation_rows = query_as::<_, OperationFieldsJoinedRow>(
            "
            SELECT
                operations_v1.public_key,
                operations_v1.document_id,
                operations_v1.operation_id,
                operations_v1.action,
                operations_v1.schema_id,
                operations_v1.previous,
                operation_fields_v1.name,
                operation_fields_v1.field_type,
                operation_fields_v1.value,
                operation_fields_v1.list_index
            FROM
                operations_v1
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = operations_v1.operation_id
            WHERE
                operations_v1.document_id = $1
            ORDER BY
                operation_fields_v1.list_index ASC
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        let mut grouped_operation_rows: BTreeMap<String, Vec<OperationFieldsJoinedRow>> =
            BTreeMap::new();

        for operation_row in operation_rows {
            if let Some(current_operations) =
                grouped_operation_rows.get_mut(&operation_row.operation_id)
            {
                current_operations.push(operation_row)
            } else {
                grouped_operation_rows
                    .insert(operation_row.clone().operation_id, vec![operation_row]);
            };
        }

        let operations: Vec<StorageOperation> = grouped_operation_rows
            .iter()
            .filter_map(|(_id, operation_rows)| parse_operation_rows(operation_rows.to_owned()))
            .collect();

        Ok(operations)
    }

    /// Get all operations that are part of a given document.
    async fn get_operations_by_schema_id(
        &self,
        id: &SchemaId,
    ) -> Result<Vec<StorageOperation>, OperationStorageError> {
        let operation_rows = query_as::<_, OperationFieldsJoinedRow>(
            "
                SELECT
                    operations_v1.public_key,
                    operations_v1.document_id,
                    operations_v1.operation_id,
                    operations_v1.action,
                    operations_v1.schema_id,
                    operations_v1.previous,
                    operation_fields_v1.name,
                    operation_fields_v1.field_type,
                    operation_fields_v1.value,
                    operation_fields_v1.list_index
                FROM
                    operations_v1
                LEFT JOIN operation_fields_v1
                    ON
                        operation_fields_v1.operation_id = operations_v1.operation_id
                WHERE
                    operations_v1.schema_id = $1
                ORDER BY
                    operation_fields_v1.list_index ASC
                ",
        )
        .bind(id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        let mut grouped_operation_rows: BTreeMap<String, Vec<OperationFieldsJoinedRow>> =
            BTreeMap::new();

        for operation_row in operation_rows {
            if let Some(current_operations) =
                grouped_operation_rows.get_mut(&operation_row.operation_id)
            {
                current_operations.push(operation_row)
            } else {
                grouped_operation_rows
                    .insert(operation_row.clone().operation_id, vec![operation_row]);
            };
        }

        let operations: Vec<StorageOperation> = grouped_operation_rows
            .iter()
            .filter_map(|(_id, operation_rows)| parse_operation_rows(operation_rows.to_owned()))
            .collect();

        Ok(operations)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::PublicKey;
    use p2panda_rs::operation::traits::{AsOperation, WithPublicKey};
    use p2panda_rs::operation::{Operation, OperationAction, OperationBuilder, OperationId};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::test_utils::constants::test_fields;
    use p2panda_rs::test_utils::fixtures::{
        document_id, operation, operation_id, operation_with_schema, public_key,
        random_document_view_id, random_operation_id, random_previous_operations, schema_id,
    };
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::test_utils::{doggo_fields, test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    #[case::create_operation(operation_with_schema(
        Some(test_fields().into()),
        None,
    ))]
    #[case::update_operation(operation_with_schema(
        Some(test_fields().into()),
        Some(random_document_view_id()),
    ))]
    #[case::update_operation_many_prev_ops(
        operation_with_schema(
            Some(test_fields().into()),
            Some(random_previous_operations(12)),
        )
    )]
    #[case::delete_operation(operation_with_schema(None, Some(random_document_view_id()),))]
    #[case::delete_operation_many_prev_ops(operation_with_schema(
        None,
        Some(random_previous_operations(12)),
    ))]
    fn insert_and_get_operations(
        #[case] operation: Operation,
        operation_id: OperationId,
        public_key: PublicKey,
        document_id: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Insert the doggo operation into the db, returns Ok(true) when succesful.
            let result = db
                .store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await;
            assert!(result.is_ok());

            // Request the previously inserted operation by it's id.
            let returned_operation = db
                .store
                .get_operation(&operation_id)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(returned_operation.public_key(), &public_key);
            assert_eq!(returned_operation.fields(), operation.fields());
            assert_eq!(
                WithId::<OperationId>::id(&returned_operation),
                &operation_id
            );
        });
    }

    #[rstest]
    fn insert_operation_twice(
        operation: Operation,
        operation_id: OperationId,
        public_key: PublicKey,
        document_id: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            db.store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await
                .unwrap();

            assert!(db
                .store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await
                .is_err());
        });
    }

    #[rstest]
    fn gets_document_by_operation_id(
        operation: Operation,
        operation_id: OperationId,
        public_key: PublicKey,
        document_id: DocumentId,
        schema_id: SchemaId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(move |db: TestDatabase| async move {
            // Getting a document by operation id which isn't stored in the database
            // should return none.
            assert!(db
                .store
                .get_document_id_by_operation_id(&operation_id)
                .await
                .expect("Get document id by operation id")
                .is_none());

            // Now we insert the operation.
            db.store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await
                .unwrap();

            // The same request should return the expected document id.
            assert_eq!(
                db.store
                    .get_document_id_by_operation_id(&operation_id)
                    .await
                    .expect("Get document id by operation id")
                    .expect("Unwrap document id"),
                document_id.clone()
            );

            // We now create and insert an update to the same document.
            let update_operation = OperationBuilder::new(&schema_id)
                .action(OperationAction::Update)
                .fields(&doggo_fields())
                .previous(&operation_id.into())
                .build()
                .expect("Builds operation");

            let update_operation_id = random_operation_id();

            db.store
                .insert_operation(
                    &update_operation_id,
                    &public_key,
                    &update_operation,
                    &document_id,
                )
                .await
                .unwrap();

            // Getting the document by the id of the new update document should also work.
            assert_eq!(
                db.store
                    .get_document_id_by_operation_id(&update_operation_id)
                    .await
                    .expect("Get document id by operation id")
                    .expect("Unwrap document id"),
                document_id.clone()
            );
        });
    }

    #[rstest]
    fn get_operations_by_document_id(
        #[from(test_db)]
        #[with(5, 1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // This is the document id of the document in the test store.
            let document_id = db.test_data.documents.first().unwrap();

            let operations_by_document_id = db
                .store
                .get_operations_by_document_id(document_id)
                .await
                .expect("Get operations by their document id");

            // We expect the number of operations returned to match the expected number.
            assert_eq!(operations_by_document_id.len(), 5)
        });
    }
}
