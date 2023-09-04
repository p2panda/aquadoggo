// SPDX-License-Identifier: AGPL-3.0-or-later

use std::num::NonZeroU64;

use bytes::{BufMut, BytesMut};
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::DocumentStore;
use sqlx::{query_scalar, AnyPool};

use crate::db::errors::{BlobStoreError, SqlStoreError};
use crate::db::query::{Filter, Order, Pagination, PaginationField, Select};
use crate::db::stores::query::{Query, RelationList};
use crate::db::SqlStore;

const BLOB_QUERY_PAGE_SIZE: u64 = 10;

pub type BlobData = Vec<u8>;

impl SqlStore {
    /// Get the data for one blob from the store, identified by it's document id.
    pub async fn get_blob(&self, id: &DocumentId) -> Result<Option<BlobData>, BlobStoreError> {
        // Get the root blob document
        let blob_document = match self.get_document(id).await? {
            Some(document) => {
                if document.schema_id != SchemaId::Blob(1) {
                    return Err(BlobStoreError::NotBlobDocument);
                }
                document
            }
            None => return Ok(None),
        };
        document_to_blob_data(self, blob_document).await
    }

    /// Get the data for one blob from the store, identified by it's document view id.
    pub async fn get_blob_by_view_id(
        &self,
        view_id: &DocumentViewId,
    ) -> Result<Option<BlobData>, BlobStoreError> {
        // Get the root blob document
        let blob_document = match self.get_document_by_view_id(view_id).await? {
            Some(document) => {
                if document.schema_id != SchemaId::Blob(1) {
                    return Err(BlobStoreError::NotBlobDocument);
                }
                document
            }
            None => return Ok(None),
        };
        document_to_blob_data(self, blob_document).await
    }

    /// Purge blob data from the node _if_ it is not related to from another document.
    pub async fn purge_blob(&self, document_id: &DocumentId) -> Result<(), SqlStoreError> {
        // Collect the view id of any existing document views which contain a relation to the blob
        // which is the purge target.
        let blob_reverse_relations = reverse_relations(&self.pool, document_id, None).await?;

        // If there are no documents referring to the blob then we continue with the purge.
        if blob_reverse_relations.is_empty() {
            // Collect the document view ids of all pieces this blob has ever referred to in it's
            // `pieces`
            let blob_piece_ids: Vec<String> = query_scalar(
                "
                SELECT
                    operation_fields_v1.value
                FROM
                    operation_fields_v1
                LEFT JOIN
                    operations_v1
                ON
                    operations_v1.operation_id = operation_fields_v1.operation_id
                WHERE
                    operations_v1.document_id = $1
                AND
                    operation_fields_v1.name = 'pieces'
                ",
            )
            .bind(document_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SqlStoreError::Transaction(e.to_string()))?;

            // Purge the blob document itself.
            self.purge_document(document_id).await?;

            // Now iterate over each collected blob piece in order to check if they are still
            // needed by any other blob document, and if not purge them as well.
            for blob_piece_id in blob_piece_ids {
                let blob_piece_id: DocumentId = blob_piece_id
                    .parse()
                    .expect("Document Id's from the store are valid");

                // Collect reverse relations for this blob piece.
                let blob_piece_reverse_relations =
                    reverse_relations(&self.pool, &blob_piece_id, Some(SchemaId::Blob(1))).await?;

                // If there are none then purge the blob piece.
                if blob_piece_reverse_relations.is_empty() {
                    self.purge_document(&blob_piece_id).await?;
                }
            }
        }

        Ok(())
    }
}

/// Helper for getting the document ids of any document which relates to the specified document.
///
/// Optionally pass in a `SchemaId` to restrict the results to documents of a certain schema.
async fn reverse_relations(
    pool: &AnyPool,
    document_id: &DocumentId,
    schema_id: Option<SchemaId>,
) -> Result<Vec<String>, SqlStoreError> {
    let schema_id_condition = match schema_id {
        Some(schema_id) => format!("AND document_views.schema_id = '{}'", schema_id),
        None => String::new(),
    };

    query_scalar(&format!(
        "
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
        LEFT JOIN
            document_views
        ON
            document_view_fields.document_view_id = document_views.document_view_id
        WHERE
            operation_fields_v1.field_type
        IN
            ('pinned_relation', 'pinned_relation_list', 'relation', 'relation_list')
        {schema_id_condition}
        AND
            operation_fields_v1.value IN (
                SELECT document_views.document_view_id
                FROM document_views
                WHERE document_views.document_id = $1
            ) OR operation_fields_v1.value = $1
        ",
    ))
    .bind(document_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| SqlStoreError::Transaction(e.to_string()))
}

/// Helper method for validation and parsing a document into blob data.
async fn document_to_blob_data(
    store: &SqlStore,
    blob: impl AsDocument,
) -> Result<Option<BlobData>, BlobStoreError> {
    // Get the length of the blob
    let expected_length = match blob.get("length").unwrap() {
        OperationValue::Integer(length) => *length as usize,
        _ => panic!(), // We should never hit this as we already validated that this is a blob document
    };

    // Get the number of pieces in the blob
    let expected_num_pieces = match blob.get("pieces").unwrap() {
        OperationValue::PinnedRelationList(list) => list.len(),
        _ => panic!(), // We should never hit this as we already validated that this is a blob document
    };

    // Now collect all existing pieces for the blob.
    //
    // We do this using the stores' query method, targeting pieces which are in the relation list
    // of the blob.
    let schema = Schema::get_system(SchemaId::BlobPiece(1)).unwrap();
    let list = RelationList::new_pinned(blob.view_id(), "pieces");

    let mut has_next_page = true;
    let mut args = Query::new(
        &Pagination::new(
            &NonZeroU64::new(BLOB_QUERY_PAGE_SIZE).unwrap(),
            None,
            &vec![PaginationField::EndCursor, PaginationField::HasNextPage],
        ),
        &Select::new(&["data".into()]),
        &Filter::default(),
        &Order::default(),
    );

    let mut buf = BytesMut::with_capacity(expected_length);
    let mut num_pieces = 0;

    while has_next_page {
        let (pagination_data, documents) = store.query(schema, &args, Some(&list)).await?;
        has_next_page = pagination_data.has_next_page;
        args.pagination.after = pagination_data.end_cursor;
        num_pieces += documents.len();

        for (_, blob_piece_document) in documents {
            match blob_piece_document
                .get("data")
                .expect("Blob piece document without \"data\" field")
            {
                // @TODO: Use bytes here instead, see related issue:
                // https://github.com/p2panda/aquadoggo/issues/543
                OperationValue::String(data_str) => buf.put(data_str.as_bytes()),
                _ => unreachable!(), // We only queried for blob piece documents
            }
        }
    }

    // No pieces were found
    if buf.is_empty() {
        return Err(BlobStoreError::NoBlobPiecesFound);
    };

    // Not all pieces were found
    if expected_num_pieces != num_pieces {
        return Err(BlobStoreError::MissingPieces);
    }

    // Combined blob data length doesn't match the claimed length
    if expected_length != buf.len() {
        return Err(BlobStoreError::IncorrectLength);
    };

    Ok(Some(buf.into()))
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_view_id};
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use rstest::rstest;

    use crate::db::errors::BlobStoreError;
    use crate::test_utils::{
        add_blob, add_document, add_schema_and_documents, assert_query, populate_and_materialize,
        populate_store_config, test_runner, update_document, TestNode,
    };

    #[rstest]
    fn get_blob(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;

            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Get blob by document id
            let blob = node.context.store.get_blob(&document_id).await.unwrap();

            assert!(blob.is_some());
            assert_eq!(blob.unwrap(), blob_data);

            // Get blob by view id
            let blob = node
                .context
                .store
                .get_blob_by_view_id(&blob_view_id)
                .await
                .unwrap();

            assert!(blob.is_some());
            assert_eq!(blob.unwrap(), blob_data)
        })
    }

    #[rstest]
    fn get_blob_errors(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".to_string();

            // Publish a blob containing pieces which aren't in the store.
            let blob_view_id = add_document(
                &mut node,
                &SchemaId::Blob(1),
                vec![
                    ("length", { blob_data.len() as i64 }.into()),
                    ("mime_type", "text/plain".into()),
                    (
                        "pieces",
                        vec![random_document_view_id(), random_document_view_id()].into(),
                    ),
                ],
                &key_pair,
            )
            .await;

            let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // We get the correct `NoBlobPiecesFound` error.
            let result = node.context.store.get_blob(&blob_document_id).await;
            assert!(
                matches!(result, Err(BlobStoreError::NoBlobPiecesFound)),
                "{:?}",
                result
            );

            // Publish one blob piece.
            let blob_piece_view_id_1 = add_document(
                &mut node,
                &SchemaId::BlobPiece(1),
                vec![("data", blob_data[..5].into())],
                &key_pair,
            )
            .await;

            // Publish a blob with one piece that is in the store and one that isn't.
            let blob_view_id = add_document(
                &mut node,
                &SchemaId::Blob(1),
                vec![
                    ("length", { blob_data.len() as i64 }.into()),
                    ("mime_type", "text/plain".into()),
                    (
                        "pieces",
                        vec![blob_piece_view_id_1.clone(), random_document_view_id()].into(),
                    ),
                ],
                &key_pair,
            )
            .await;

            let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // We should get the correct `MissingBlobPieces` error.
            let result = node.context.store.get_blob(&blob_document_id).await;
            assert!(
                matches!(result, Err(BlobStoreError::MissingPieces)),
                "{:?}",
                result
            );

            // Publish one more blob piece, but it doesn't contain the correct number of bytes.
            let blob_piece_view_id_2 = add_document(
                &mut node,
                &SchemaId::BlobPiece(1),
                vec![("data", blob_data[9..].into())],
                &key_pair,
            )
            .await;

            // Publish a blob with two pieces that are in the store but they don't add up to the
            // right byte length.
            let blob_view_id = add_document(
                &mut node,
                &SchemaId::Blob(1),
                vec![
                    ("length", { blob_data.len() as i64 }.into()),
                    ("mime_type", "text/plain".into()),
                    (
                        "pieces",
                        vec![blob_piece_view_id_1, blob_piece_view_id_2].into(),
                    ),
                ],
                &key_pair,
            )
            .await;

            let blob_document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // We get the correct `IncorrectLength` error.
            let result = node.context.store.get_blob(&blob_document_id).await;
            assert!(
                matches!(result, Err(BlobStoreError::IncorrectLength)),
                "{:?}",
                result
            );
        })
    }

    #[rstest]
    fn purge_blob(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 7, "text/plain", &key_pair).await;

            // There is one blob and two blob pieces in database.
            //
            // These are the rows we expect to exist in each table.
            assert_query(&node, "SELECT entry_hash FROM entries", 3).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 3).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 6).await;
            assert_query(&node, "SELECT log_id FROM logs", 3).await;
            assert_query(&node, "SELECT document_id FROM documents", 3).await;
            assert_query(&node, "SELECT document_id FROM document_views", 3).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 5).await;

            // Purge this blob from the database, we now expect all tables to be empty (except the
            // logs table).
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
            let result = node.context.store.purge_blob(&document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 0).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 0).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 0).await;
            assert_query(&node, "SELECT log_id FROM logs", 3).await;
            assert_query(&node, "SELECT document_id FROM documents", 0).await;
            assert_query(&node, "SELECT document_id FROM document_views", 0).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 0).await;

            let result = node.context.store.purge_blob(&document_id).await;

            assert!(result.is_ok(), "{:#?}", result)
        })
    }

    #[rstest]
    fn purge_blob_only_purges_blobs(
        #[from(populate_store_config)]
        #[with(1, 1, 1)]
        config: PopulateStoreConfig,
        key_pair: KeyPair,
    ) {
        test_runner(|mut node: TestNode| async move {
            let _ = populate_and_materialize(&mut node, &config).await;

            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 7, "text/plain", &key_pair).await;

            // There is one blob and two blob pieces in database.
            //
            // These are the rows we expect to exist in each table.
            assert_query(&node, "SELECT entry_hash FROM entries", 4).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 4).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 19).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 4).await;
            assert_query(&node, "SELECT document_id FROM document_views", 4).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 15).await;

            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
            let result = node.context.store.purge_blob(&document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 1).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 1).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 13).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 1).await;
            assert_query(&node, "SELECT document_id FROM document_views", 1).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 10).await;

            let result = node.context.store.purge_blob(&document_id).await;

            assert!(result.is_ok(), "{:#?}", result)
        })
    }

    #[rstest]
    fn does_not_purge_blob_if_still_pinned(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 7, "text/plain", &key_pair).await;

            let _ = add_schema_and_documents(
                &mut node,
                "img",
                vec![vec![(
                    "blob",
                    blob_view_id.clone().into(),
                    Some(SchemaId::Blob(1)),
                )]],
                &key_pair,
            )
            .await;

            assert_query(&node, "SELECT entry_hash FROM entries", 6).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 6).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 12).await;
            assert_query(&node, "SELECT log_id FROM logs", 6).await;
            assert_query(&node, "SELECT document_id FROM documents", 6).await;
            assert_query(&node, "SELECT document_id FROM document_views", 6).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 11).await;

            // Purge this blob from the database, we now expect all tables to be empty.
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
            let result = node.context.store.purge_blob(&document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 6).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 6).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 12).await;
            assert_query(&node, "SELECT log_id FROM logs", 6).await;
            assert_query(&node, "SELECT document_id FROM documents", 6).await;
            assert_query(&node, "SELECT document_id FROM document_views", 6).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 11).await;

            let result = node.context.store.purge_blob(&document_id).await;

            assert!(result.is_ok(), "{:#?}", result)
        })
    }

    #[rstest]
    fn purge_all_pieces_of_updated_blob(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 7, "text/plain", &key_pair).await;

            // Create a new blob piece.
            let new_blob_pieces = add_document(
                &mut node,
                &SchemaId::BlobPiece(1),
                vec![("data", "more blob data".into())],
                &key_pair,
            )
            .await;

            // Update the blob document to point at the new blob piece.
            let _ = update_document(
                &mut node,
                &SchemaId::Blob(1),
                vec![("pieces", vec![new_blob_pieces].into())],
                &blob_view_id,
                &key_pair,
            )
            .await;

            // There is one blob and three blob pieces in database.
            //
            // These are the rows we expect to exist in each table.
            assert_query(&node, "SELECT entry_hash FROM entries", 5).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 5).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 8).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 4).await;
            assert_query(&node, "SELECT document_id FROM document_views", 5).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 9).await;

            // Purge this blob from the database, we now expect all tables to be empty (except the
            // logs table).
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
            let result = node.context.store.purge_blob(&document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 0).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 0).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 0).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 0).await;
            assert_query(&node, "SELECT document_id FROM document_views", 0).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 0).await;

            let result = node.context.store.purge_blob(&document_id).await;

            assert!(result.is_ok(), "{:#?}", result)
        })
    }
}
