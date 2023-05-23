// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fmt::Display;

use async_trait::async_trait;
use p2panda_rs::document::DocumentId;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::traits::AsOperation;
use p2panda_rs::operation::{Operation, OperationId};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::error::OperationStorageError;
use p2panda_rs::storage_provider::traits::OperationStore;
use sqlx::{query, query_as, query_scalar, Any};

use crate::db::models::utils::{parse_operation_rows, parse_value_to_string_vec};
use crate::db::models::{DocumentViewFieldRow, OperationFieldsJoinedRow};
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        // Construct query for inserting operation an row, execute it and check exactly one row was
        // affected.
        query(
            "
            INSERT INTO
                operations_v1 (
                    public_key,
                    document_id,
                    operation_id,
                    action,
                    schema_id,
                    previous,
                    sorted_index
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, null)
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
        .execute(&mut tx)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        let mut results = Vec::new();
        if let Some(fields) = operation.fields() {
            for (name, value) in fields.iter() {
                // If the value is a relation_list or pinned_relation_list we need to insert a new
                // field row for every item in the list. Here we collect these items and return
                // them in a vector. If this operation value is anything except for the above list
                // types, we will return a vec containing a single item.
                let db_values = parse_value_to_string_vec(value);

                for (index, db_value) in db_values.into_iter().enumerate() {
                    let cursor = OperationCursor::new(index, name, id);

                    let result = query(
                        "
                        INSERT INTO
                            operation_fields_v1 (
                                operation_id,
                                name,
                                field_type,
                                value,
                                list_index,
                                cursor
                            )
                        VALUES
                            ($1, $2, $3, $4, $5, $6)
                        ",
                    )
                    .bind(id.as_str().to_owned())
                    .bind(name.to_owned())
                    .bind(value.field_type().to_string())
                    .bind(db_value)
                    .bind(index as i32)
                    .bind(cursor.to_string())
                    .execute(&mut tx)
                    .await
                    .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

                    results.push(result);
                }
            }
        };

        // Commit the transaction.
        tx.commit()
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
                operations_v1.sorted_index,
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
                operations_v1.sorted_index,
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
                -- order the operations by their index when topologically sorted, in the case where this may not be set yet
                -- we fall back to ordering by operation id. In both cases we additionally order by list index.
                operations_v1.sorted_index ASC, operations_v1.operation_id ASC, operation_fields_v1.list_index ASC
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        if operation_rows.is_empty() {
            return Ok(vec![]);
        }

        Ok(group_and_parse_operation_rows(operation_rows))
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
                    operations_v1.sorted_index,
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
                    operations_v1.operation_id ASC, operation_fields_v1.list_index ASC
                ",
        )
        .bind(id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        if operation_rows.is_empty() {
            return Ok(vec![]);
        }

        Ok(group_and_parse_operation_rows(operation_rows))
    }
}

/// Parse a collection of operation rows from multiple operations, into a list of `StorageOperation`.
///
/// Expects the rows to be grouped by operation id.
fn group_and_parse_operation_rows(
    operation_rows: Vec<OperationFieldsJoinedRow>,
) -> Vec<StorageOperation> {
    // We need to group all the operation rows so they can be parsed into operations.
    // They come from the database ordered by their index once topologically sorted when
    // present, otherwise by operation id. List items are additionally ordered by their
    // list index.

    let mut grouped_operation_rows = vec![];

    let mut current_operation_id = operation_rows.first().unwrap().operation_id.clone();
    let mut current_operation_rows = vec![];

    let mut operation_rows_iter = operation_rows.into_iter();
    while let Some(row) = operation_rows_iter.next() {
        if row.operation_id == current_operation_id {
            // If this row is part of the current operation push it to the current rows vec.
            current_operation_rows.push(row);
        } else {
            // If we've moved on to the next operation, then push the complete vec of
            // operation rows to the grouped rows collection and then setup for the next
            // iteration.
            grouped_operation_rows.push(current_operation_rows.clone());
            current_operation_id = row.operation_id.clone();
            current_operation_rows = vec![row];
        }
    }

    // Push the final operation to the grouped rows.
    grouped_operation_rows.push(current_operation_rows);

    // Parse all the operation rows into operations.
    grouped_operation_rows
        .into_iter()
        .filter_map(|operation_rows| parse_operation_rows(operation_rows.to_owned()))
        .collect()
}

impl SqlStore {
    pub async fn update_operation_index(
        &self,
        operation_id: &OperationId,
        sorted_index: i32,
    ) -> Result<(), OperationStorageError> {
        query::<Any>(
            "
            UPDATE 
                operations_v1
            SET
                sorted_index = $2
            WHERE
                operation_id = $1
            ",
        )
        .bind(operation_id.as_str())
        .bind(sorted_index)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::FatalStorageError(e.to_string()))?;

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OperationCursor(String);

impl OperationCursor {
    /// Generate an unique cursor for each document.
    ///
    /// This helps us to aid cursor-based pagination, especially over duplicate documents in
    /// relation lists
    pub fn new(list_index: usize, field_name: &str, operation_id: &OperationId) -> Self {
        let cursor =
            Hash::new_from_bytes(format!("{list_index}{field_name}{operation_id}").as_bytes());
        Self(cursor.as_str()[4..].to_owned())
    }
}

impl Display for OperationCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for OperationCursor {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&DocumentViewFieldRow> for OperationCursor {
    fn from(row: &DocumentViewFieldRow) -> Self {
        Self::new(
            row.list_index as usize,
            &row.name,
            &row.operation_id.parse().unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::materialization::build_graph;
    use p2panda_rs::document::{DocumentBuilder, DocumentId};
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
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use p2panda_rs::WithId;
    use rstest::rstest;

    use crate::test_utils::{
        doggo_fields, populate_and_materialize, populate_store_config, test_runner, TestNode,
    };

    use super::OperationCursor;

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
    ) {
        test_runner(move |node: TestNode| async move {
            // Insert the doggo operation into the db, returns Ok(true) when succesful.
            let result = node
                .context
                .store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await;
            assert!(result.is_ok());

            // Request the previously inserted operation by it's id.
            let returned_operation = node
                .context
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
    ) {
        test_runner(move |node: TestNode| async move {
            node.context
                .store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await
                .unwrap();

            assert!(node
                .context
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
    ) {
        test_runner(move |node: TestNode| async move {
            // Getting a document by operation id which isn't stored in the database
            // should return none.
            assert!(node
                .context
                .store
                .get_document_id_by_operation_id(&operation_id)
                .await
                .expect("Get document id by operation id")
                .is_none());

            // Now we insert the operation.
            node.context
                .store
                .insert_operation(&operation_id, &public_key, &operation, &document_id)
                .await
                .unwrap();

            // The same request should return the expected document id.
            assert_eq!(
                node.context
                    .store
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

            node.context
                .store
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
                node.context
                    .store
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
        #[from(populate_store_config)]
        #[with(10, 1, 1)]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store with some entries and operations and materialize documents.
            let (_, document_ids) = populate_and_materialize(&mut node, &config).await;
            let document_id = document_ids.get(0).expect("At least one document id");

            let operations_by_document_id = node
                .context
                .store
                .get_operations_by_document_id(document_id)
                .await
                .expect("Get operations by their document id");

            // We expect the number of operations returned to match the expected number.
            assert_eq!(operations_by_document_id.len(), 10);

            // The operations should be in their topologically sorted order.
            let operations = DocumentBuilder::from(&operations_by_document_id).operations();
            let expected_operation_order = build_graph(&operations).unwrap().sort().unwrap().sorted();

            assert_eq!(operations, expected_operation_order);
        });
    }

    #[rstest]
    fn operation_cursor(operation_id: OperationId) {
        let cursor = OperationCursor::new(5, "username", &operation_id);
        assert_eq!(cursor.to_string().len(), 64);
    }
}
