// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::ResolverContext;
use async_graphql::Error;
use dynamic_graphql::FieldValue;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema};
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::stores::{PaginationCursor, PaginationData, RelationList};
use crate::db::types::StorageDocument;
use crate::db::SqlStore;
use crate::graphql::objects::DocumentMeta;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::{get_document_from_params, gql_scalar, parse_collection_arguments};
use crate::schema::SchemaProvider;

/// Document data passed between resolvers.
#[derive(Clone, Debug)]
pub enum Resolved {
    /// Single document.
    Document(StorageDocument),

    /// Collection of multiple documents, with pagination data.
    Collection(
        PaginationData<PaginationCursor>,
        Vec<(PaginationCursor, StorageDocument)>,
    ),

    /// Single document as part of a collection, with pagination data.
    CollectionDocument(PaginationCursor, StorageDocument),
}

impl Resolved {
    /// Downcast document value which will have been passed up by the parent query node, retrieved
    /// via the `ResolverContext`.
    ///
    /// We unwrap internally here as we expect validation to have occured in the query resolver.
    pub fn downcast(ctx: &ResolverContext) -> Self {
        ctx.parent_value
            .downcast_ref::<Self>()
            .expect("Values passed from query parent should match expected")
            .to_owned()
    }
}

pub async fn resolve_document(
    ctx: ResolverContext<'_>,
    document_id: Option<DocumentIdScalar>,
    document_view_id: Option<DocumentViewIdScalar>,
) -> Result<Option<FieldValue>, Error> {
    let store = ctx.data_unchecked::<SqlStore>();

    let document = match get_document_from_params(store, &document_id, &document_view_id).await? {
        Some(document) => Resolved::Document(document),
        None => return Ok(FieldValue::NONE),
    };

    // Pass it up to resolve all fields of document
    Ok(Some(FieldValue::owned_any(document)))
}

/// Resolve a collection of documents.
///
/// This collection can be either resolved by schema (root collection), or via a relation list
/// (nested collection).
pub async fn resolve_document_collection(
    ctx: ResolverContext<'_>,
    schema: Schema,
    list: Option<RelationList>,
) -> Result<Option<FieldValue>, Error> {
    let store = ctx.data_unchecked::<SqlStore>();

    // Populate query arguments with values from GraphQL query
    let query = parse_collection_arguments(&ctx, &schema, &list)?;

    // Fetch all queried documents and compose the value to be passed up the query tree
    let (pagination_data, documents) = store.query(&schema, &query, list.as_ref()).await?;
    let collection = Resolved::Collection(pagination_data, documents);

    Ok(Some(FieldValue::owned_any(collection)))
}

/// Resolve meta fields of a single document.
pub async fn resolve_document_meta(
    ctx: ResolverContext<'_>,
) -> Result<Option<FieldValue<'_>>, Error> {
    let document = Resolved::downcast(&ctx);

    // Extract the document in the case of a single or paginated request
    let document = match document {
        Resolved::Document(document) => document,
        Resolved::CollectionDocument(_, document) => document,
        Resolved::Collection(_, _) => panic!("Expected list item or single document"),
    };

    // We defined the document meta type and registered it in the GraphQL schema
    let document_meta = DocumentMeta {
        document_id: document.id().into(),
        document_view_id: document.view_id().into(),
        owner: document.author().to_owned().into(),
    };

    Ok(Some(FieldValue::owned_any(document_meta)))
}

/// Resolve a single document field value.
///
/// If the value is a relation, then the relevant document id or document view id is determined and
/// passed along the query chain. If the value is a simple type (meaning it is also a query leaf)
/// then it is directly resolved.
pub async fn resolve_document_field(
    ctx: ResolverContext<'_>,
) -> Result<Option<FieldValue<'_>>, Error> {
    let store = ctx.data_unchecked::<SqlStore>();
    let schema_provider = ctx.data_unchecked::<SchemaProvider>();

    // Parse the bubble up value
    let document = match Resolved::downcast(&ctx) {
        Resolved::Document(document) => document,
        Resolved::CollectionDocument(_, document) => document,
        Resolved::Collection(_, _) => panic!("Expected list item or single document"),
    };

    let schema = schema_provider
        .get(document.schema_id())
        .await
        .expect("Schema should be in store");

    // Determine name of the field to be resolved
    let name = ctx.field().name();

    match document.get(name).unwrap() {
        // Relation fields are expected to resolve to the related document
        OperationValue::Relation(relation) => {
            let document = match store.get_document(relation.document_id()).await? {
                Some(document) => document,
                None => return Ok(FieldValue::NONE),
            };

            let document = Resolved::Document(document);
            Ok(Some(FieldValue::owned_any(document)))
        }
        // Pinned relation behaves the same as relation but passes along a document view id
        OperationValue::PinnedRelation(relation) => {
            let document = match store.get_document_by_view_id(relation.view_id()).await? {
                Some(document) => document,
                None => return Ok(FieldValue::NONE),
            };

            let document = Resolved::Document(document);
            Ok(Some(FieldValue::owned_any(document)))
        }
        // Relation lists are handled by collecting and returning a list of all document ids in
        // the relation list
        OperationValue::RelationList(_) => {
            // Get the schema of documents in this relation list
            let relation_field_schema = schema
                .fields()
                .get(name)
                .expect("Document field should exist on schema");

            let schema = match relation_field_schema {
                FieldType::RelationList(schema_id) => {
                    // We can unwrap here as the schema should exist in the store already
                    schema_provider.get(schema_id).await.unwrap()
                }
                _ => panic!("Schema should exist"),
            };

            // Select relation field containing list of documents
            let list = RelationList::new_unpinned(document.view_id(), name);

            resolve_document_collection(ctx, schema, Some(list)).await
        }
        // Pinned relation lists behave the same as relation lists but pass along view ids
        OperationValue::PinnedRelationList(_) => {
            // Get the schema of documents in this relation list
            let relation_field_schema = schema
                .fields()
                .get(name)
                .expect("Document field should exist on schema");

            let schema = match relation_field_schema {
                FieldType::PinnedRelationList(schema_id) => {
                    // We can unwrap here as the schema should exist in the store
                    // already
                    schema_provider.get(schema_id).await.unwrap()
                }
                _ => panic!(), // Should never reach here.
            };

            // Select relation field containing list of pinned document views
            let list = RelationList::new_pinned(document.view_id(), name);

            resolve_document_collection(ctx, schema, Some(list)).await
        }
        // All other fields are simply resolved to their scalar value
        value => Ok(Some(FieldValue::value(gql_scalar(value)))),
    }
}
