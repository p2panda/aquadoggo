// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use async_graphql::Value;
use p2panda_rs::schema::Schema;

use crate::graphql::constants;
use crate::graphql::utils::{downcast_document, paginated_document_name, paginated_response_name};

#[derive(Default, Clone, Debug)]
pub struct PaginationData {
    total_count: u64,
    has_next_page: bool,
}

pub struct PaginatedResponse;

impl PaginatedResponse {
    pub fn build(schema: &Schema) -> Object {
        Object::new(paginated_response_name(schema.id()))
            .field(Field::new(
                constants::TOTAL_COUNT_FIELD,
                TypeRef::named_nn(TypeRef::INT),
                move |ctx| {
                    FieldFuture::new(async move {
                        let document_value = downcast_document(&ctx);

                        let total_count = match document_value {
                            super::DocumentValue::Single(_) => panic!("Expected paginated value"),
                            super::DocumentValue::Paginated(_, data, _) => data.total_count,
                        };

                        Ok(Some(FieldValue::from(Value::from(total_count))))
                    })
                },
            ))
            .field(Field::new(
                constants::HAS_NEXT_PAGE_FIELD,
                TypeRef::named_nn(TypeRef::BOOLEAN),
                move |ctx| {
                    FieldFuture::new(async move {
                        let document_value = downcast_document(&ctx);

                        let has_next_page = match document_value {
                            super::DocumentValue::Single(_) => panic!("Expected paginated value"),
                            super::DocumentValue::Paginated(_, data, _) => data.has_next_page,
                        };

                        Ok(Some(FieldValue::from(Value::from(has_next_page))))
                    })
                },
            ))
            .field(Field::new(
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
            ))
    }
}
