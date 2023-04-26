// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use async_graphql::Value;
use p2panda_rs::schema::Schema;

use crate::db::query::Cursor;
use crate::graphql::constants;
use crate::graphql::resolvers::Resolved;
use crate::graphql::utils::{collection_item_name, collection_name};

/// Dynamically build objects describing a paginated collection of documents.
///
/// Each object contains `documents`, `totalCount` and `hasNextPage` fields and defines their
/// resolution logic.
///
/// Each generated object has a type name with the formatting `<schema_id>Collection`.
pub fn build_document_collection_object(schema: &Schema) -> Object {
    Object::new(collection_name(schema.id()))
        .field(
            Field::new(
                constants::TOTAL_COUNT_FIELD,
                TypeRef::named_nn(TypeRef::INT),
                move |ctx| {
                    FieldFuture::new(async move {
                        let collection = Resolved::downcast(&ctx);

                        let total_count = match collection {
                            Resolved::Collection(page_info, _) => page_info.total_count,
                            _ => panic!("Expected document collection"),
                        };

                        Ok(Some(FieldValue::from(Value::from(
                            total_count.expect("Value needs to be set when requested"),
                        ))))
                    })
                },
            )
            .description("The total number of documents available in this paginated collection."),
        )
        .field(
            Field::new(
                constants::END_CURSOR_FIELD,
                TypeRef::named_nn(TypeRef::STRING),
                move |ctx| {
                    FieldFuture::new(async move {
                        let collection = Resolved::downcast(&ctx);

                        let end_cursor = match collection {
                            Resolved::Collection(page_info, _) => page_info.end_cursor,
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
                        let collection = Resolved::downcast(&ctx);

                        let has_next_page = match collection {
                            Resolved::Collection(page_info, _) => page_info.has_next_page,
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
                        let collection = Resolved::downcast(&ctx);
                        let documents = match collection {
                            Resolved::Collection(_, documents) => documents,
                            _ => panic!("Expected document collection"),
                        };

                        let documents_list: Vec<FieldValue> = documents
                            .into_iter()
                            .map(|(cursor, document)| {
                                FieldValue::owned_any(Resolved::CollectionDocument(
                                    cursor, document,
                                ))
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
