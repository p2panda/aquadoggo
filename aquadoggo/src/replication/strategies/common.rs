use p2panda_rs::{
    document::{traits::AsDocument, DocumentId},
    schema::{Schema, SchemaId},
    storage_provider::traits::DocumentStore,
};

use crate::{db::SqlStore, replication::SchemaIdSet, schema::SchemaProvider};

pub fn has_blob_relation(schema: &Schema) -> bool {
    for (_, field_type) in schema.fields().iter() {
        match field_type {
            p2panda_rs::schema::FieldType::Relation(schema_id)
            | p2panda_rs::schema::FieldType::RelationList(schema_id)
            | p2panda_rs::schema::FieldType::PinnedRelation(schema_id)
            | p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
                if schema_id == &SchemaId::Blob(1) {
                    return true;
                }
            }
            _ => (),
        }
    }
    false
}

/// Calculate the documents which should be included in this replication session.
///
/// This is based on the schema ids included in the target set and any document dependencies
/// which we have on our local node. Documents which are of type `blob_v1` are only included
/// if the `blob_v1` schema is included in the target set _and_ the blob document is related
/// to from another document (also of a schema included in the target set). The same is true
/// of `blob_piece_v1` documents. These are only included if a blob document is also included
/// which relates to them.
///
/// For example, a target set including the schema id `[img_0020, blob_v1]` would look at all
/// `img_0020` documents and only include blobs which they relate to.
pub async fn included_document_ids(
    store: &SqlStore,
    schema_provider: &SchemaProvider,
    target_set: &SchemaIdSet,
) -> Vec<DocumentId> {
    let wants_blobs = target_set.contains(&SchemaId::Blob(1));
    let wants_blob_pieces = target_set.contains(&SchemaId::BlobPiece(1));
    let mut all_target_documents = vec![];
    let mut all_blob_documents = vec![];
    let mut all_blob_piece_documents = vec![];
    for schema_id in target_set.iter() {
        // If the schema is `blob_v1` or `blob_piece_v1` we don't take any action and just
        // move onto the next loop as these types of documents are only included as part of
        // other application documents.
        if schema_id == &SchemaId::Blob(1) || schema_id == &SchemaId::BlobPiece(1) {
            continue;
        }

        // Check if documents of this type contain a relation to a blob document.
        let has_blob_relation = match schema_provider.get(schema_id).await {
            Some(schema) => has_blob_relation(&schema),
            None => false,
        };

        // Get the ids of all documents of this schema.
        let schema_documents: Vec<DocumentId> = store
            .get_documents_by_schema(schema_id)
            .await
            .unwrap()
            .iter()
            .map(|document| document.id())
            .cloned()
            .collect();

        let mut schema_blob_documents = vec![];

        // If the target set included `blob_v1` schema_id then we collect any related blob documents.
        if wants_blobs && has_blob_relation {
            for document_id in &schema_documents {
                let blob_documents = store.get_blob_child_relations(document_id).await.unwrap();
                schema_blob_documents.extend(blob_documents)
            }
        }

        // If `blob_piece_v1` is included in the target set.
        if wants_blob_pieces && has_blob_relation {
            for blob_id in &schema_blob_documents {
                // Get all existing views for this blob document.
                let blob_document_view_ids = store
                    .get_all_document_view_ids(blob_id)
                    .await
                    .expect("Fatal database error");
                for blob_view_id in blob_document_view_ids {
                    // Get all pieces for each blob view.
                    let blob_piece_ids = store
                        .get_child_document_ids(&blob_view_id)
                        .await
                        .expect("Fatal database error");
                    all_blob_piece_documents.extend(blob_piece_ids)
                }
            }
        }

        all_target_documents.extend(schema_documents);
        all_blob_documents.extend(schema_blob_documents);
    }

    let mut all_included_document_ids = vec![];
    all_included_document_ids.extend(all_target_documents);
    all_included_document_ids.extend(all_blob_documents);
    all_included_document_ids.extend(all_blob_piece_documents);

    all_included_document_ids
}
