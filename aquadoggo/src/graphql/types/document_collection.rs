// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, ResolverContext, TypeRef};
use async_graphql::{Error, Value};
use p2panda_rs::schema::Schema;

use crate::db::query::Cursor;
use crate::db::stores::RelationList;
use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::types::DocumentValue;
use crate::graphql::utils::{
    collection_item_name, collection_name, downcast_document, parse_collection_arguments,
};

/// Pagination data passed from parent to child query fields.
#[derive(Default, Clone, Debug)]
pub struct PaginationData<C>
where
    C: Cursor,
{
    /// Number of all documents in queried collection.
    pub total_count: Option<u64>,

    /// Flag indicating if `endCursor` will return another page.
    pub has_next_page: bool,

    /// Flag indicating if `startCursor` will return another page.
    pub has_previous_page: bool,

    /// Cursor which can be used to paginate backwards.
    pub start_cursor: Option<C>,

    /// Cursor which can be used to paginate forwards.
    pub end_cursor: Option<C>,
}

/// A constructor for dynamically building objects describing a paginated collection of documents.
/// Each object contains a `documents`, `totalCount` and `hasNextPage` fields and defines their
/// resolution logic. Each generated object has a type name with the formatting
/// `<schema_id>Collection`.
///
/// A type should be added to the root GraphQL schema for every schema supported on a node, as
/// these types are not known at compile time we make use of the `async-graphql` `dynamic` module.
pub struct DocumentCollection;

impl DocumentCollection {
    pub fn build(schema: &Schema) -> Object {
        Object::new(collection_name(schema.id()))
            .field(
                Field::new(
                    constants::TOTAL_COUNT_FIELD,
                    TypeRef::named_nn(TypeRef::INT),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let document_value = downcast_document(&ctx);

                            let total_count = match document_value {
                                DocumentValue::Collection(data, _) => data.total_count,
                                _ => panic!("Expected document collection"),
                            };

                            Ok(Some(FieldValue::from(Value::from(
                                total_count.expect("Value needs to be set when requested"),
                            ))))
                        })
                    },
                )
                .description(
                    "The total number of documents available in this paginated collection.",
                ),
            )
            .field(
                Field::new(
                    constants::END_CURSOR_FIELD,
                    TypeRef::named_nn(TypeRef::STRING),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let document_value = downcast_document(&ctx);

                            let end_cursor = match document_value {
                                DocumentValue::Collection(data, _) => data.end_cursor,
                                _ => panic!("Expected document collection"),
                            };

                            match end_cursor {
                                Some(cursor) => {
                                    Ok(Some(FieldValue::from(Value::from(cursor.encode()))))
                                }
                                None => Ok(Some(FieldValue::NULL)),
                            }
                        })
                    },
                )
                .description("Cursor for the next page"),
            )
            .field(
                Field::new(
                    constants::HAS_NEXT_PAGE_FIELD,
                    TypeRef::named_nn(TypeRef::BOOLEAN),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let document_value = downcast_document(&ctx);

                            let has_next_page = match document_value {
                                DocumentValue::Collection(data, _) => data.has_next_page,
                                _ => panic!("Expected document collection"),
                            };

                            Ok(Some(FieldValue::from(Value::from(has_next_page))))
                        })
                    },
                )
                .description(
                    "Boolean value denoting whether there is a next page available on this query.",
                ),
            )
            .field(
                Field::new(
                    constants::DOCUMENTS_FIELD,
                    TypeRef::named_nn_list_nn(collection_item_name(schema.id())),
                    move |ctx| {
                        FieldFuture::new(async move {
                            // Here we just pass up the root query parameters to be used in the fields
                            // resolver
                            let document_value = downcast_document(&ctx);
                            let documents = match document_value {
                                DocumentValue::Collection(_, documents) => documents,
                                _ => panic!("Expected document collection"),
                            };

                            let documents_list: Vec<FieldValue> = documents
                                .into_iter()
                                .map(|(cursor, document)| {
                                    FieldValue::owned_any(DocumentValue::Item(cursor, document))
                                })
                                .collect();

                            // Pass the list up to the children fields
                            Ok(Some(FieldValue::list(documents_list)))
                        })
                    },
                )
                .description("Field containing the actual document fields."),
            )
            .description(format!(
                "A single page response returned when querying a collection of `{}` documents.",
                schema.id().name()
            ))
    }

    /// Resolver to be used when processing a query for a document collection.
    pub async fn resolve(
        ctx: ResolverContext<'_>,
        schema: Schema,
        list: Option<RelationList>,
    ) -> Result<Option<FieldValue>, Error> {
        let store = ctx.data_unchecked::<SqlStore>();

        // Populate query arguments with values from GraphQL query
        let query = parse_collection_arguments(&ctx, &schema)?;

        // Fetch all queried documents and compose the value to be passed up the query tree
        let (pagination_data, documents) = store.query(&schema, &query, list.as_ref()).await?;
        let collection = DocumentValue::Collection(pagination_data, documents);

        Ok(Some(FieldValue::owned_any(collection)))
    }
}
