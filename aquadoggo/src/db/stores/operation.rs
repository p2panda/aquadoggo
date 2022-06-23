// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::DocumentId;
use p2panda_rs::operation::{AsOperation, AsVerifiedOperation, OperationId, VerifiedOperation};
use p2panda_rs::storage_provider::errors::OperationStorageError;
use p2panda_rs::storage_provider::traits::OperationStore;
use sqlx::{query, query_as, query_scalar};

use crate::db::models::OperationFieldsJoinedRow;
use crate::db::provider::SqlStorage;
use crate::db::utils::{parse_operation_rows, parse_value_to_string_vec};

/// Implementation of `OperationStore` trait which is required when constructing a
/// `StorageProvider`.
///
/// Handles storage and retrieval of operations in the form of `VerifiedOperation` which
/// implements the required `AsVerifiedOperation` trait.
///
/// There are several intermediary structs defined in `db/models/` which represent
/// rows from tables in the database where this entry, it's fields and opreation
/// relations are stored. These are used in conjunction with the `sqlx` library
/// to coerce raw values into structs when querying the database.
#[async_trait]
impl OperationStore<VerifiedOperation> for SqlStorage {
    /// Get the id of the document an operation is part of.
    ///
    /// Returns a result containing a `DocumentId` wrapped in an option. If no
    /// document was found, then this method returns None. Errors if a fatal
    /// storage error occurs.
    async fn get_document_by_operation_id(
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
    /// This requires a DoggoOperation to be composed elsewhere, it contains an `Author`,
    /// `DocumentId`, `OperationId` and the actual `Operation` we want to store.
    ///
    /// Returns a result containing `true` when one insertion occured, and false
    /// when no insertions occured. Errors when a fatal storage error occurs.
    ///
    /// In aquadoggo we store an operation in the database in three different tables:
    /// `operations`, `previous_operations` and `operation_fields`. This means that
    /// this method actually makes 3 different sets of insertions.
    async fn insert_operation(
        &self,
        operation: &VerifiedOperation,
        document_id: &DocumentId,
    ) -> Result<(), OperationStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Consruct query for inserting operation an row, execute it
        // and check exactly one row was affected.
        let operation_insertion_result = query(
            "
            INSERT INTO
                operations_v1 (
                    author,
                    document_id,
                    operation_id,
                    entry_hash,
                    action,
                    schema_id,
                    previous_operations
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(operation.public_key().as_str())
        .bind(document_id.as_str())
        .bind(operation.operation_id().as_str())
        .bind(operation.operation_id().as_hash().as_str())
        .bind(operation.action().as_str())
        .bind(operation.schema().as_str())
        .bind(
            operation
                .previous_operations()
                .map(|document_view_id| document_view_id.as_str()),
        )
        .execute(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Construct and execute the queries, return their futures and execute
        // all of them with `try_join_all()`.
        let fields_insertion_result = match operation.fields() {
            Some(fields) => {
                let result = try_join_all(fields.iter().flat_map(|(name, value)| {
                    // If the value is a relation_list or pinned_relation_list we need to insert a new field row for
                    // every item in the list. Here we collect these items and return them in a vector. If this operation
                    // value is anything except for the above list types, we will return a vec containing a single item.
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
                            .bind(operation.operation_id().as_str().to_owned())
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
            return Err(OperationStorageError::InsertionError(
                operation.operation_id().clone(),
            ));
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
    /// Returns a result containing an `VerifiedOperation` wrapped in an option, if no
    /// operation with this id was found, returns none. Errors if a fatal storage
    /// error occured.
    async fn get_operation_by_id(
        &self,
        id: &OperationId,
    ) -> Result<Option<VerifiedOperation>, OperationStorageError> {
        let operation_rows = query_as::<_, OperationFieldsJoinedRow>(
            "
            SELECT
                operations_v1.author,
                operations_v1.document_id,
                operations_v1.operation_id,
                operations_v1.entry_hash,
                operations_v1.action,
                operations_v1.schema_id,
                operations_v1.previous_operations,
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
    ) -> Result<Vec<VerifiedOperation>, OperationStorageError> {
        let operation_rows = query_as::<_, OperationFieldsJoinedRow>(
            "
            SELECT
                operations_v1.author,
                operations_v1.document_id,
                operations_v1.operation_id,
                operations_v1.entry_hash,
                operations_v1.action,
                operations_v1.schema_id,
                operations_v1.previous_operations,
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

        let operations: Vec<VerifiedOperation> = grouped_operation_rows
            .iter()
            .filter_map(|(_id, operation_rows)| parse_operation_rows(operation_rows.to_owned()))
            .collect();

        Ok(operations)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentId;
    use p2panda_rs::entry::LogId;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{
        AsOperation, AsVerifiedOperation, Operation, OperationId, VerifiedOperation,
    };
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::storage_provider::traits::{AsStorageEntry, EntryStore, StorageProvider};
    use p2panda_rs::test_utils::constants::{default_fields, DEFAULT_HASH};
    use p2panda_rs::test_utils::fixtures::{
        create_operation, delete_operation, document_id, key_pair, operation_fields, operation_id,
        public_key, random_previous_operations, update_operation, verified_operation,
    };
    use rstest::rstest;

    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};

    #[rstest]
    #[case::create_operation(create_operation(&default_fields()))]
    #[case::update_operation(update_operation(&default_fields(), &DEFAULT_HASH.parse().unwrap()))]
    #[case::update_operation_many_prev_ops(update_operation(&default_fields(), &random_previous_operations(12)))]
    #[case::delete_operation(delete_operation(&DEFAULT_HASH.parse().unwrap()))]
    #[case::delete_operation_many_prev_ops(delete_operation(&random_previous_operations(12)))]
    fn insert_get_operations(
        #[case] operation: Operation,
        #[from(public_key)] author: Author,
        operation_id: OperationId,
        document_id: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Construct the storage operation.
            let operation = VerifiedOperation::new(&author, &operation_id, &operation).unwrap();

            // Insert the doggo operation into the db, returns Ok(true) when succesful.
            let result = db.store.insert_operation(&operation, &document_id).await;
            assert!(result.is_ok());

            // Request the previously inserted operation by it's id.
            let returned_operation = db
                .store
                .get_operation_by_id(operation.operation_id())
                .await
                .unwrap()
                .unwrap();

            assert_eq!(returned_operation.public_key(), operation.public_key());
            assert_eq!(returned_operation.fields(), operation.fields());
            assert_eq!(returned_operation.operation_id(), operation.operation_id());
        });
    }

    #[rstest]
    fn insert_operation_twice(
        #[from(verified_operation)] verified_operation: VerifiedOperation,
        document_id: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let _result = db
                .store
                .insert_operation(&verified_operation, &document_id)
                .await;

            assert!(db
                .store
                .insert_operation(&verified_operation, &document_id)
                .await
                .is_err());

            // @TODO
            // assert_eq!(
            //     db.store.insert_operation(&verified_operation, &document_id).await.unwrap_err().to_string(),
            //     "A fatal error occured in OperationStore: error returned from database: UNIQUE constraint failed: operations_v1.entry_hash"
            // )
        });
    }

    #[rstest]
    fn gets_document_by_operation_id(
        #[from(verified_operation)]
        #[with(Some(operation_fields(default_fields())), None, None, None, Some(DEFAULT_HASH.parse().unwrap()))]
        create_operation: VerifiedOperation,
        #[from(verified_operation)]
        #[with(Some(operation_fields(default_fields())), Some(DEFAULT_HASH.parse().unwrap()))]
        update_operation: VerifiedOperation,
        document_id: DocumentId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            assert!(db
                .store
                .get_document_by_operation_id(create_operation.operation_id())
                .await
                .unwrap()
                .is_none());

            db.store
                .insert_operation(&create_operation, &document_id)
                .await
                .unwrap();

            assert_eq!(
                db.store
                    .get_document_by_operation_id(create_operation.operation_id())
                    .await
                    .unwrap()
                    .unwrap(),
                document_id.clone()
            );

            db.store
                .insert_operation(&update_operation, &document_id)
                .await
                .unwrap();

            assert_eq!(
                db.store
                    .get_document_by_operation_id(create_operation.operation_id())
                    .await
                    .unwrap()
                    .unwrap(),
                document_id.clone()
            );
        });
    }

    #[rstest]
    fn get_operations_by_document_id(
        key_pair: KeyPair,
        #[from(test_db)]
        #[with(5, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

            let latest_entry = db
                .store
                .get_latest_entry(&author, &LogId::default())
                .await
                .unwrap()
                .unwrap();

            let document_id = db
                .store
                .get_document_by_entry(&latest_entry.hash())
                .await
                .unwrap()
                .unwrap();

            let operations_by_document_id = db
                .store
                .get_operations_by_document_id(&document_id)
                .await
                .unwrap();

            assert_eq!(operations_by_document_id.len(), 5)
        });
    }
}
