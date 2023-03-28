// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use async_graphql::Value;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::utils::{downcast_document, paginated_document_name, paginated_response_name};

/// Pagination data passed from parent to child query fields.
#[derive(Default, Clone, Debug)]
pub struct PaginationData {
    /// The total page count.
    total_count: u64,

    /// Whether this query has a next page waiting.
    has_next_page: bool,
}

/// A constructor for dynamically building objects describing a paginated collection of documents.
/// Each object contains a `document`, `totalCount` and `hasNextPage` fields and defines their
/// resolution logic. Each generated object has a type name with the formatting `<schema_id>PaginatedResponse`.
///
/// A type should be added to the root GraphQL schema for every schema supported on a node, as
/// these types are not known at compile time we make use of the `async-graphql` `dynamic` module.
pub struct PaginatedResponse;

impl PaginatedResponse {
    pub fn build(schema: &Schema) -> Object {
        Object::new(paginated_response_name(schema.id()))
            .field(
                Field::new(
                    constants::TOTAL_COUNT_FIELD,
                    TypeRef::named_nn(TypeRef::INT),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let document_value = downcast_document(&ctx);

                            let total_count = match document_value {
                                super::DocumentValue::Single(_) => {
                                    panic!("Expected paginated value")
                                }
                                super::DocumentValue::Paginated(_, data, _) => data.total_count,
                            };

                            Ok(Some(FieldValue::from(Value::from(total_count))))
                        })
                    },
                )
                .description(
                    "The total number of documents available in this paginated collection.",
                ),
            )
            .field(
                Field::new(
                    constants::HAS_NEXT_PAGE_FIELD,
                    TypeRef::named_nn(TypeRef::BOOLEAN),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let document_value = downcast_document(&ctx);

                            let has_next_page = match document_value {
                                super::DocumentValue::Single(_) => {
                                    panic!("Expected paginated value")
                                }
                                super::DocumentValue::Paginated(_, data, _) => data.has_next_page,
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
                    constants::DOCUMENT_FIELD,
                    TypeRef::named(paginated_document_name(schema.id())),
                    move |ctx| {
                        FieldFuture::new(async move {
                            // Here we just pass up the root query parameters to be used in the fields
                            // resolver
                            let document_value = downcast_document(&ctx);
                            Ok(Some(FieldValue::owned_any(document_value)))
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
}
