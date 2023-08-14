// SPDX-License-Identifier: AGPL-3.0-or-later

use std::num::NonZeroU64;

use async_stream::try_stream;
use bytes::{BufMut, BytesMut};
use futures::Stream;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::validate::MAX_BLOB_PIECE_LENGTH;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::DocumentStore;
use sqlx::{query_scalar, AnyPool};

use crate::db::errors::{BlobStoreError, SqlStoreError};
use crate::db::query::{Filter, Order, Pagination, PaginationField, Select};
use crate::db::stores::query::{PaginationCursor, Query, RelationList};
use crate::db::SqlStore;

/// Number of blob pieces requested per database query iteration.
const BLOB_QUERY_PAGE_SIZE: u64 = 10;

pub type BlobData = Vec<u8>;

/// Gets blob data from the database in chunks (via pagination) and populates a readable stream
/// with it.
///
/// This stream can further be used to write data into a file etc. This helps dealing with large
/// blobs as only little system memory is occupied per reading and writing step. We only move small
/// chunks at a time and keep the memory-footprint managable.
///
/// Currently the BLOB_QUERY_PAGE_SIZE is set to 10 which is the multiplier of the
/// MAX_BLOB_PIECE_LENGTH. With 10 * 256kb we occupy an approximate maximum of 2.56mb memory at a
/// time. If these values make sense needs to be re-visited, but it is a start!
#[derive(Debug)]
pub struct BlobStream {
    store: SqlStore,
    pagination_cursor: Option<PaginationCursor>,
    document_view_id: DocumentViewId,
    num_pieces: usize,
    length: usize,
    expected_num_pieces: usize,
    expected_length: usize,
}

impl BlobStream {
    pub fn new(store: &SqlStore, document: impl AsDocument) -> Result<Self, BlobStoreError> {
        if document.schema_id() != &SchemaId::Blob(1) {
            return Err(BlobStoreError::NotBlobDocument);
        }

        // Get the length of the blob
        let expected_length = match document.get("length").unwrap() {
            OperationValue::Integer(length) => *length as usize,
            _ => unreachable!(), // We already validated that this is a blob document
        };

        // Get the number of pieces in the blob
        let expected_num_pieces = match document.get("pieces").unwrap() {
            OperationValue::PinnedRelationList(list) => list.len(),
            _ => unreachable!(), // We already validated that this is a blob document
        };

        Ok(Self {
            store: store.to_owned(),
            pagination_cursor: None,
            document_view_id: document.view_id().to_owned(),
            num_pieces: 0,
            length: 0,
            expected_length,
            expected_num_pieces,
        })
    }

    async fn next_chunk(&mut self) -> Result<BlobData, BlobStoreError> {
        let schema = Schema::get_system(SchemaId::BlobPiece(1)).expect("System schema is given");
        let list = RelationList::new_pinned(&self.document_view_id, "pieces");

        let args = Query::new(
            &Pagination::new(
                &NonZeroU64::new(BLOB_QUERY_PAGE_SIZE).unwrap(),
                self.pagination_cursor.as_ref(),
                &vec![PaginationField::EndCursor, PaginationField::HasNextPage],
            ),
            &Select::new(&["data".into()]),
            &Filter::default(),
            &Order::default(),
        );

        let mut buf =
            BytesMut::with_capacity(BLOB_QUERY_PAGE_SIZE as usize * MAX_BLOB_PIECE_LENGTH);

        let (pagination_data, documents) = self.store.query(schema, &args, Some(&list)).await?;
        self.pagination_cursor = pagination_data.end_cursor;
        self.num_pieces += documents.len();

        for (_, blob_piece_document) in documents {
            match blob_piece_document
                .get("data")
                .expect("Blob piece document without \"data\" field")
            {
                OperationValue::Bytes(data_str) => buf.put(&data_str[..]),
                _ => unreachable!(), // We only queried for blob piece documents
            }
        }

        self.length += buf.len();

        Ok(buf.to_vec())
    }

    /// This method is called _after_ the stream has ended. We compare the values with what we've
    /// expected and find inconsistencies and invalid blobs.
    fn validate(&self) -> Result<(), BlobStoreError> {
        // Not all pieces were found
        if self.expected_num_pieces != self.num_pieces {
            return Err(BlobStoreError::MissingPieces);
        }

        // Combined blob data length doesn't match the claimed length
        if self.expected_length != self.length {
            return Err(BlobStoreError::IncorrectLength);
        };

        Ok(())
    }

    /// Establishes a data stream of blob data.
    ///
    /// The stream ends when all data has been written, at the end the blob data gets validated
    /// against the expected blob length.
    ///
    /// To consume this stream in form of an iterator it is required to use the `pin_mut` macro.
    // NOTE: Clippy does not understand that this macro generates code which asks for an explicit
    // lifetime.
    #[allow(clippy::needless_lifetimes)]
    pub fn read_all<'a>(&'a mut self) -> impl Stream<Item = Result<BlobData, BlobStoreError>> + 'a {
        try_stream! {
            loop {
                let blob_data = self.next_chunk().await?;

                if blob_data.is_empty() {
                    self.validate()?;
                    break;
                }

                yield blob_data;
            }
        }
    }
}

impl SqlStore {
    /// Get data stream for one blob from the store, identified by it's document id.
    pub async fn get_blob(&self, id: &DocumentId) -> Result<Option<BlobStream>, BlobStoreError> {
        if let Some(document) = self.get_document(id).await? {
            let document = validate_blob_pieces(self, document).await?;
            Ok(Some(BlobStream::new(self, document)?))
        } else {
            Ok(None)
        }
    }

    /// Get data stream for one blob from the store, identified by its document view id.
    pub async fn get_blob_by_view_id(
        &self,
        view_id: &DocumentViewId,
    ) -> Result<Option<BlobStream>, BlobStoreError> {
        if let Some(document) = self.get_document_by_view_id(view_id).await? {
            let document = validate_blob_pieces(self, document).await?;
            Ok(Some(BlobStream::new(self, document)?))
        } else {
            Ok(None)
        }
    }

    /// Purge blob data from the node _if_ it is not related to from another document.
    pub async fn purge_blob(&self, document_id: &DocumentId) -> Result<(), SqlStoreError> {
        // Collect the view id of any existing document views which contain a relation to the blob
        // which is the purge target.
        let blob_reverse_relations = reverse_relations(&self.pool, document_id, None).await?;

        // If there are no documents referring to the blob then we continue with the purge.
        if blob_reverse_relations.is_empty() {
            // Collect the document view ids of all pieces this blob has ever referred to in its
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

    pub async fn get_blob_child_relations(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<DocumentId>, SqlStoreError> {
        let document_ids: Vec<String> = query_scalar(&format!(
            "
            SELECT DISTINCT 
                document_views.document_id
            FROM
                document_views
            WHERE 
                document_views.schema_id = 'blob_v1' 
            AND 
                document_views.document_view_id 
            IN (
                SELECT
                    operation_fields_v1.value
                FROM 
                    document_view_fields
                LEFT JOIN 
                    {OPERATION_FIELDS}
                LEFT JOIN 
                    {DOCUMENT_VIEWS}
                WHERE
                    operation_fields_v1.field_type IN ('pinned_relation', 'pinned_relation_list', 'relation_list', 'relation')
                AND 
                    document_views.document_id = $1
            )
            "
        ))
        .bind(document_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| SqlStoreError::Transaction(err.to_string()))?;

        Ok(document_ids
            .iter()
            .map(|document_id_str| {
                document_id_str
                    .parse::<DocumentId>()
                    .expect("Document Id's coming from the store should be valid")
            })
            .collect())
    }
}

/// Throws an error when database does not contain all related blob pieces yet.
async fn validate_blob_pieces(
    store: &SqlStore,
    document: impl AsDocument,
) -> Result<impl AsDocument, BlobStoreError> {
    let schema = Schema::get_system(SchemaId::BlobPiece(1)).expect("System schema is given");
    let list = RelationList::new_pinned(document.view_id(), "pieces");

    let args = Query::new(
        &Pagination::new(
            &NonZeroU64::new(BLOB_QUERY_PAGE_SIZE).unwrap(),
            None,
            &vec![PaginationField::TotalCount],
        ),
        &Select::default(),
        &Filter::default(),
        &Order::default(),
    );

    let (pagination_data, _) = store.query(schema, &args, Some(&list)).await?;
    let total_count = pagination_data
        .total_count
        .expect("total count is selected");

    // Get the number of pieces in the blob
    let expected_num_pieces = match document.get("pieces").unwrap() {
        OperationValue::PinnedRelationList(list) => list.len(),
        _ => unreachable!(), // We already validated that this is a blob document
    };

    if total_count as usize == expected_num_pieces {
        Ok(document)
    } else {
        Err(BlobStoreError::MissingPieces)
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

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use futures::{pin_mut, StreamExt};
    use p2panda_rs::document::DocumentId;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::{key_pair, random_document_view_id};
    use p2panda_rs::test_utils::generate_random_bytes;
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use rstest::rstest;

    use crate::db::errors::BlobStoreError;
    use crate::test_utils::{
        add_blob, add_document, add_schema_and_documents, assert_query, populate_and_materialize,
        populate_store_config, test_runner, update_document, TestNode,
    };

    use super::BlobStream;

    async fn read_data_from_stream(mut blob_stream: BlobStream) -> Result<Vec<u8>, BlobStoreError> {
        let stream = blob_stream.read_all();
        pin_mut!(stream);

        let mut buf = BytesMut::new();

        while let Some(value) = stream.next().await {
            match value {
                Ok(blob_data) => {
                    buf.put(blob_data.as_slice());
                }
                Err(err) => return Err(err),
            }
        }

        Ok(buf.to_vec())
    }

    #[rstest]
    fn get_blob(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = "Hello, World!".as_bytes();
            let blob_view_id = add_blob(&mut node, &blob_data, 6, "text/plain", &key_pair).await;
            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();

            // Get blob by document id
            let blob_stream = node.context.store.get_blob(&document_id).await.unwrap();
            assert!(blob_stream.is_some());
            let collected_data = read_data_from_stream(blob_stream.unwrap()).await;
            assert_eq!(blob_data, collected_data.unwrap());

            // Get blob by view id
            let blob_stream_view = node
                .context
                .store
                .get_blob_by_view_id(&blob_view_id)
                .await
                .unwrap();
            assert!(blob_stream_view.is_some());
            let collected_data = read_data_from_stream(blob_stream_view.unwrap()).await;
            assert_eq!(blob_data, collected_data.unwrap());
        })
    }

    #[rstest]
    fn get_blob_errors(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let blob_data = generate_random_bytes(12);

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
            let stream = node.context.store.get_blob(&blob_document_id).await;
            assert!(matches!(stream, Err(BlobStoreError::MissingPieces)));

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
            let stream = node.context.store.get_blob(&blob_document_id).await;
            assert!(matches!(stream, Err(BlobStoreError::MissingPieces)));

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
            let stream = node
                .context
                .store
                .get_blob(&blob_document_id)
                .await
                .unwrap();
            let collected_data = read_data_from_stream(stream.unwrap()).await;
            assert!(matches!(
                collected_data,
                Err(BlobStoreError::IncorrectLength)
            ),);
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
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 20).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 4).await;
            assert_query(&node, "SELECT document_id FROM document_views", 4).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 16).await;

            let document_id: DocumentId = blob_view_id.to_string().parse().unwrap();
            let result = node.context.store.purge_blob(&document_id).await;
            assert!(result.is_ok(), "{:#?}", result);
            assert_query(&node, "SELECT entry_hash FROM entries", 1).await;
            assert_query(&node, "SELECT operation_id FROM operations_v1", 1).await;
            assert_query(&node, "SELECT operation_id FROM operation_fields_v1", 14).await;
            assert_query(&node, "SELECT log_id FROM logs", 4).await;
            assert_query(&node, "SELECT document_id FROM documents", 1).await;
            assert_query(&node, "SELECT document_id FROM document_views", 1).await;
            assert_query(&node, "SELECT name FROM document_view_fields", 11).await;

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
                vec![("data", "more blob data".as_bytes().into())],
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
