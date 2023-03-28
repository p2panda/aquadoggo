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

/// A constructor for dynamically building objects describing documents which conform to the shape
/// of a p2panda schema. Each object contains contains `fields` and `meta` fields and defines
/// their resolution logic.
///
/// A type should be added to the root GraphQL schema for every schema supported on a node, as
/// these types are not known at compile time we make use of the `async-graphql` `dynamic` module.
///
/// See `DocumentFields` and `DocumentMeta` to see the shape of the children field types.
pub struct DocumentSchema;

impl DocumentSchema {
    /// Build a GraphQL object type from a p2panda schema.
    ///
    /// Constructs resolvers for both `fields` and `meta` fields. The former simply passes up the query
    /// arguments to it's children query fields. The latter calls the `resolve` method defined on
    /// `DocumentMeta` type.
    pub fn build(schema: &Schema) -> Object {
        let fields = Object::new(schema.id().to_string());
        with_document_fields(fields, &schema)
    }
}

/// A constructor for dynamically building objects describing documents which conform to the shape
/// of a p2panda schema and are contained in a paginated collection. Each object contains
/// contains `fields`, `meta` and `cursor` fields and defines their resolution logic.
///
/// A type should be added to the root GraphQL schema for every schema supported on a node, as
/// these types are not known at compile time we make use of the `async-graphql` `dynamic` module.
///
/// See `DocumentFields` and `DocumentMeta` to see the shape of the children field types.
pub struct PaginatedDocumentSchema;

impl PaginatedDocumentSchema {
    /// Build a GraphQL object type from a p2panda schema.
    ///
    /// Contains resolvers for both `fields` and `meta`. The former simply passes up the query
    /// arguments to it's children query fields. The latter calls the `resolve` method defined on
    /// `DocumentMeta` type.
    pub fn build(schema: &Schema) -> Object {
        let fields = Object::new(paginated_document_name(schema.id()));
        with_document_fields(fields, &schema).field(
            Field::new(
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
            )
            .description(format!("The pagination `cursor` for this `{}` document.", schema.id().name())),
        )
    }
}

/// Add application and meta fields to a schema type object.
fn with_document_fields(fields: Object, schema: &Schema) -> Object {
    fields // The `fields` field passes down the parent value to it's children.
        .field(
            Field::new(
                constants::FIELDS_FIELD,
                TypeRef::named(fields_name(schema.id())),
                move |ctx| {
                    FieldFuture::new(async move {
                        // Downcast the document which has already been retrieved from the store
                        // by the root query resolver and passed down to the `fields` field here.
                        let document_value = downcast_document(&ctx);

                        // We continue to pass it down to all the fields' children.
                        Ok(Some(FieldValue::owned_any(document_value)))
                    })
                },
            )
            .description(format!("Application fields of a `{}` document.", schema.id().name())),
        )
        // The `meta` field of a document, resolves the `DocumentMeta` object.
        .field(
            Field::new(
                constants::META_FIELD,
                TypeRef::named(constants::DOCUMENT_META),
                move |ctx| FieldFuture::new(async move { DocumentMeta::resolve(ctx).await }),
            )
            .description(format!("Meta fields of a `{}` document.", schema.id().name())),
        )
        .description(schema.description().to_string())
}
