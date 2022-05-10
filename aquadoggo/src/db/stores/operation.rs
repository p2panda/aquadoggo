// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::future::try_join_all;
use p2panda_rs::document::DocumentId;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    AsOperation, Operation, OperationAction, OperationFields, OperationId,
};
use p2panda_rs::schema::SchemaId;
use sqlx::{query, query_as, query_scalar};

use crate::db::errors::OperationStorageError;
use crate::db::models::operation::OperationFieldsJoinedRow;
use crate::db::provider::SqlStorage;
use crate::db::traits::{AsStorageOperation, OperationStore, PreviousOperations};
use crate::db::utils::{parse_operation_rows, parse_value_to_string_vec};

#[derive(Debug, Clone)]
pub struct OperationStorage {
    author: Author,
    operation: Operation,
    id: OperationId,
    document_id: DocumentId,
}

impl OperationStorage {
    pub fn new(
        author: &Author,
        operation: &Operation,
        operation_id: &OperationId,
        document_id: &DocumentId,
    ) -> Self {
        Self {
            author: author.clone(),
            operation: operation.clone(),
            id: operation_id.clone(),
            document_id: document_id.clone(),
        }
    }
}

impl AsStorageOperation for OperationStorage {
    type AsStorageOperationError = OperationStorageError;

    fn action(&self) -> OperationAction {
        self.operation.action()
    }

    fn author(&self) -> Author {
        self.author.clone()
    }

    fn id(&self) -> OperationId {
        self.id.clone()
    }

    fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    fn schema_id(&self) -> SchemaId {
        self.operation.schema()
    }

    fn fields(&self) -> Option<OperationFields> {
        self.operation.fields()
    }

    fn previous_operations(&self) -> PreviousOperations {
        self.operation.previous_operations().unwrap_or_default()
    }
}

#[async_trait]
impl OperationStore<OperationStorage> for SqlStorage {
    /// Get the id of the document an operation is part of.
    ///
    /// Returns a result containing a `DocumentId` wrapped in an option. If no
    /// document was found, then this method returns None. Errors if a fatal
    /// storage error occurs.
    async fn get_document_by_operation_id(
        &self,
        id: OperationId,
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
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        Ok(document_id.map(|id_str| id_str.parse().unwrap()))
    }

    /// Insert an operation into storage.
    ///
    /// This requires a DoggoOperation to be composed elsewhere, it contains an Author,
    /// DocumentId, OperationId and the actual Operation we want to store.
    ///
    /// Returns a result containing `true` when one insertion occured, and false
    /// when no insertions occured. Errors when a fatal storage error occurs.
    ///
    /// In aquadoggo we store an operatin in the database in three different tables:
    /// `operations`, `previous_operations` and `operation_fields`. This means that
    /// this method actually makes 3 different sets of insertions.
    async fn insert_operation(
        &self,
        operation: &OperationStorage,
    ) -> Result<(), OperationStorageError> {
        // Start a transaction, any db insertions after this point, and before the `commit()`
        // will be rolled back in the event of an error.
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        // TODO: Once we have resolved https://github.com/p2panda/p2panda/issues/315 then
        // we can derive this string from the previous_operations' `DocumentViewId`
        let mut prev_op_string = "".to_string();
        for (i, operation_id) in operation.previous_operations().iter().enumerate() {
            let separator = if i == 0 { "" } else { "_" };
            prev_op_string += format!("{}{}", separator, operation_id.as_hash().as_str()).as_str();
        }

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
        .bind(operation.author().as_str())
        .bind(operation.document_id().as_str())
        .bind(operation.id().as_str())
        .bind(operation.id().as_hash().as_str())
        .bind(operation.action().as_str())
        .bind(operation.schema_id().as_str())
        .bind(prev_op_string.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        // Loop over all previous operations and insert one row for
        // each, construct and execute the queries, return their futures
        // and execute all of them with `try_join_all()`.
        let previous_operations_insertion_result =
            try_join_all(operation.previous_operations().iter().map(|prev_op_id| {
                query(
                    "
                    INSERT INTO
                        previous_operations_v1 (
                            parent_operation_id,
                            child_operation_id
                        )
                    VALUES
                        ($1, $2)
                    ",
                )
                .bind(prev_op_id.as_str())
                .bind(operation.id().as_str().to_owned())
                .execute(&self.pool)
            }))
            .await
            // If any of database errors occur we will catch that here
            .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        // Same pattern as above but now for operation_fields. Construct and execute the
        // queries, return their futures and execute all of them with `try_join_all()`.
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
                        .map(|db_value| {
                            // Compose the query and return it's future.
                            query(
                                "
                            INSERT INTO
                                operation_fields_v1 (
                                    operation_id,
                                    name,
                                    field_type,
                                    value
                                )
                            VALUES
                                ($1, $2, $3, $4)
                            ",
                            )
                            .bind(operation.id().as_str().to_owned())
                            .bind(name.to_owned())
                            .bind(value.field_type().to_string())
                            .bind(db_value)
                            .execute(&self.pool)
                        })
                        .collect::<Vec<_>>()
                }))
                .await
                // If any queries error, we catch that here.
                .map_err(|e| OperationStorageError::Custom(e.to_string()))?;
                Some(result)
            }
            None => None,
        };

        // Check every insertion performed affected exactly 1 row.
        if operation_insertion_result.rows_affected() != 1
            || previous_operations_insertion_result
                .iter()
                .any(|query_result| query_result.rows_affected() != 1)
            || fields_insertion_result
                .unwrap_or_default()
                .iter()
                .any(|query_result| query_result.rows_affected() != 1)
        {
            return Err(OperationStorageError::InsertionError(operation.id()));
        }

        // Commit the transaction.
        transaction
            .commit()
            .await
            .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        Ok(())
    }

    /// Get an operation identified by it's OperationId.
    ///
    /// Returns a result containing an OperationStorage wrapped in an option, if no
    /// operation with this id was found, returns none. Errors if a fatal storage
    /// error occured.
    async fn get_operation_by_id(
        &self,
        id: OperationId,
    ) -> Result<Option<OperationStorage>, OperationStorageError> {
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
                operation_fields_v1.value
            FROM
                operations_v1
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = operations_v1.operation_id
            WHERE
                operations_v1.operation_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

        let operation = parse_operation_rows(operation_rows);
        Ok(operation)
    }

    async fn get_operations_by_document_id(
        &self,
        id: &DocumentId,
    ) -> Result<Vec<OperationStorage>, OperationStorageError> {
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
                operation_fields_v1.value
            FROM
                operations_v1
            LEFT JOIN operation_fields_v1
                ON
                    operation_fields_v1.operation_id = operations_v1.operation_id
            WHERE
                operations_v1.document_id = $1
            ",
        )
        .bind(id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OperationStorageError::Custom(e.to_string()))?;

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

        let operations: Vec<OperationStorage> = grouped_operation_rows
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
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::OperationId;
    use p2panda_rs::storage_provider::traits::{EntryStore, StorageProvider};
    use p2panda_rs::test_utils::constants::{DEFAULT_HASH, DEFAULT_PRIVATE_KEY};

    use crate::db::provider::SqlStorage;
    use crate::db::stores::test_utils::{
        test_create_operation, test_db, test_delete_operation, test_update_operation,
    };
    use crate::db::traits::AsStorageOperation;

    use super::{OperationStorage, OperationStore};

    async fn insert_get_assert(storage_provider: SqlStorage, operation: OperationStorage) {
        // Insert the doggo operation into the db, returns Ok(true) when succesful.
        let result = storage_provider.insert_operation(&operation).await;
        assert!(result.is_ok());

        // Request the previously inserted operation by it's id.
        let returned_operation = storage_provider
            .get_operation_by_id(operation.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(returned_operation.author(), operation.author());
        assert_eq!(returned_operation.fields(), operation.fields());
        assert_eq!(returned_operation.id(), operation.id());
        assert_eq!(returned_operation.document_id(), operation.document_id());
    }

    #[tokio::test]
    async fn insert_get_create_operation() {
        let storage_provider = test_db(0, false).await;

        // Create Author, OperationId and DocumentId in order to compose a OperationStorage.
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let create_operation = OperationStorage::new(
            &author,
            &test_create_operation(),
            &operation_id,
            &document_id,
        );

        insert_get_assert(storage_provider, create_operation).await;
    }

    #[tokio::test]
    async fn insert_get_update_operation() {
        let storage_provider = test_db(0, false).await;

        // Create Author, OperationId and DocumentId in order to compose a OperationStorage.
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let prev_op_id = DEFAULT_HASH.parse().unwrap();

        let update_operation = OperationStorage::new(
            &author,
            &test_update_operation(vec![prev_op_id], "huhuhu"),
            &operation_id,
            &document_id,
        );
        insert_get_assert(storage_provider, update_operation).await;
    }

    #[tokio::test]
    async fn insert_get_delete_operation() {
        let storage_provider = test_db(0, false).await;

        // Create Author, OperationId and DocumentId in order to compose a OperationStorage.
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let prev_op_id = DEFAULT_HASH.parse().unwrap();

        let delete_operation = OperationStorage::new(
            &author,
            &test_delete_operation(vec![prev_op_id]),
            &operation_id,
            &document_id,
        );

        insert_get_assert(storage_provider, delete_operation).await;
    }

    #[tokio::test]
    async fn insert_operation_twice() {
        let storage_provider = test_db(0, false).await;

        // Create Author, OperationId and DocumentId in order to compose a OperationStorage.
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());
        let create_operation = OperationStorage::new(
            &author,
            &test_create_operation(),
            &operation_id,
            &document_id,
        );

        let result = storage_provider.insert_operation(&create_operation).await;

        assert!(result.is_ok());

        let result = storage_provider.insert_operation(&create_operation).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Error occured in OperationStore: error returned from database: UNIQUE constraint failed: operations_v1.entry_hash"
        )
    }

    #[tokio::test]
    async fn gets_document_by_operation_id() {
        let storage_provider = test_db(0, false).await;

        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();
        let operation_id = OperationId::new(DEFAULT_HASH.parse().unwrap());
        let document_id = DocumentId::new(operation_id.clone());

        let create_operation = OperationStorage::new(
            &author,
            &test_create_operation(),
            &operation_id,
            &document_id,
        );

        let document_id_should_be_none = storage_provider
            .get_document_by_operation_id(operation_id.clone())
            .await
            .unwrap();

        assert!(document_id_should_be_none.is_none());

        storage_provider
            .insert_operation(&create_operation)
            .await
            .unwrap();

        let expected_document_id: DocumentId = create_operation.id().as_hash().clone().into();
        let document_id_should_exist = storage_provider
            .get_document_by_operation_id(operation_id.clone())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(document_id_should_exist, expected_document_id);

        let operation_id = OperationId::new(Hash::new_from_bytes(vec![3, 4, 5]).unwrap());

        let update_operation = OperationStorage::new(
            &author,
            &test_update_operation(
                vec![create_operation.id().as_hash().clone().into()],
                "huhuhu",
            ),
            &operation_id,
            &document_id,
        );

        storage_provider
            .insert_operation(&update_operation)
            .await
            .unwrap();

        let document_id_should_be_the_same = storage_provider
            .get_document_by_operation_id(update_operation.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(document_id_should_be_the_same, expected_document_id);
    }

    #[tokio::test]
    async fn get_operations_by_document_id() {
        let storage_provider = test_db(5, false).await;
        let key_pair = KeyPair::from_private_key_str(DEFAULT_PRIVATE_KEY).unwrap();
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        let latest_entry = storage_provider
            .latest_entry(&author, &LogId::default())
            .await
            .unwrap()
            .unwrap();
        let document_id = storage_provider
            .get_document_by_entry(&Hash::new(&latest_entry.entry_hash).unwrap())
            .await
            .unwrap()
            .unwrap();

        let operations_by_document_id = storage_provider
            .get_operations_by_document_id(&document_id)
            .await
            .unwrap();

        assert_eq!(operations_by_document_id.len(), 5)
    }
}
