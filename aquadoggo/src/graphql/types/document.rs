// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use async_graphql::Value;
use p2panda_rs::schema::Schema;

use crate::db::types::StorageDocument;
use crate::graphql::constants;
use crate::graphql::types::{DocumentMeta, PaginationData};
use crate::graphql::utils::{downcast_document, fields_name, paginated_document_name};

#[derive(Clone, Debug)]
pub enum DocumentValue {
    Single(StorageDocument),
    Paginated(String, PaginationData, StorageDocument),
}

/// GraphQL object which represents a document type which contains `fields` and `meta` fields. A
/// type is added to the root GraphQL schema for every document, as these types are not known at
/// compile time we make use of the `async-graphql ` `dynamic` module.
///
/// See `DocumentFields` and `DocumentMeta` to see the shape of the children field types.
pub struct DocumentSchema;

impl DocumentSchema {
    /// Build a GraphQL object type from a p2panda schema.
    ///
    /// Contains resolvers for both `fields` and `meta`. The former simply passes up the query
    /// arguments to it's children query fields. The latter calls the `resolve` method defined on
    /// `DocumentMeta` type.
    pub fn build(schema: &Schema) -> Object {
        let document_fields_name = fields_name(schema.id());
        Object::new(schema.id().to_string())
            // The `fields` field of a document, passes up the query arguments to it's children.
            .field(Field::new(
                constants::FIELDS_FIELD,
                TypeRef::named(document_fields_name),
                move |ctx| {
                    FieldFuture::new(async move {
                        // Here we just pass up the root query parameters to be used in the fields
                        // resolver
                        let document_value = downcast_document(&ctx);
                        Ok(Some(FieldValue::owned_any(document_value)))
                    })
                },
            ))
            // The `meta` field of a document, resolves the `DocumentMeta` object.
            .field(Field::new(
                constants::META_FIELD,
                TypeRef::named(constants::DOCUMENT_META),
                move |ctx| FieldFuture::new(async move { DocumentMeta::resolve(ctx).await }),
            ))
            .description(schema.description().to_string())
    }
}

pub struct PaginatedDocumentSchema;

impl PaginatedDocumentSchema {
    /// Build a GraphQL object type from a p2panda schema.
    ///
    /// Contains resolvers for both `fields` and `meta`. The former simply passes up the query
    /// arguments to it's children query fields. The latter calls the `resolve` method defined on
    /// `DocumentMeta` type.
    pub fn build(schema: &Schema) -> Object {
        let document_fields_name = fields_name(schema.id());
        Object::new(paginated_document_name(schema.id()))
            // The `fields` field of a document, passes up the query arguments to it's children.
            .field(Field::new(
                constants::FIELDS_FIELD,
                TypeRef::named(document_fields_name),
                move |ctx| {
                    FieldFuture::new(async move {
                        // Here we just pass up the root query parameters to be used in the fields
                        // resolver
                        let document_value = downcast_document(&ctx);
                        Ok(Some(FieldValue::owned_any(document_value)))
                    })
                },
            ))
            // The `meta` field of a document, resolves the `DocumentMeta` object.
            .field(Field::new(
                constants::META_FIELD,
                TypeRef::named(constants::DOCUMENT_META),
                move |ctx| FieldFuture::new(async move { DocumentMeta::resolve(ctx).await }),
            ))
            .field(Field::new(
                constants::CURSOR_FIELD,
                TypeRef::named(TypeRef::STRING),
                move |ctx| {
                    FieldFuture::new(async move {
                        let document_value = downcast_document(&ctx);

                        let cursor = match &document_value {
                            DocumentValue::Single(_) => panic!("Paginated document expected"),
                            DocumentValue::Paginated(cursor, _, _) => cursor,
                        };

                        Ok(Some(FieldValue::from(Value::String(cursor.to_owned()))))
                    })
                },
            ))
    }
}
