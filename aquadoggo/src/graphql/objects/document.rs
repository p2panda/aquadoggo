// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use async_graphql::Value;
use p2panda_rs::schema::Schema;

use crate::db::query::Cursor;
use crate::graphql::constants;
use crate::graphql::resolvers::{resolve_document_meta, Resolved};
use crate::graphql::utils::{collection_item_name, fields_name};

/// Dynamically build GraphQL objects describing documents which conform to the shape of a p2panda
/// schema.
///
/// Constructs resolvers for both `fields` and `meta` fields. The former simply passes up the query
/// arguments to its children query fields. The latter calls the `resolve` method defined on
/// `DocumentMeta` type.
pub fn build_document_object(schema: &Schema) -> Object {
    let fields = Object::new(schema.id().to_string());
    with_document_fields(fields, schema)
}

/// Dynamically build GraphQL objects describing documents which conform to the shape of a p2panda
/// schema and are contained in a paginated collection.
///
/// Contains resolvers for `cursor`, `fields` and `meta`. `fields` simply passes up the query
/// arguments to its children query fields. `meta` calls the `resolve` method defined on
/// `DocumentMeta` type.
pub fn build_paginated_document_object(schema: &Schema) -> Object {
    let fields = Object::new(collection_item_name(schema.id()));

    with_document_fields(fields, schema).field(
        Field::new(
            constants::CURSOR_FIELD,
            TypeRef::named(TypeRef::STRING),
            move |ctx| {
                FieldFuture::new(async move {
                    let document = Resolved::downcast(&ctx);

                    let cursor = match &document {
                        Resolved::CollectionDocument(cursor, _) => cursor,
                        _ => panic!("Paginated document expected"),
                    };

                    Ok(Some(FieldValue::from(Value::String(cursor.encode()))))
                })
            },
        )
        .description(format!(
            "The pagination cursor for this `{}` document.",
            schema.id().name()
        )),
    )
}

/// Add application `fields` and `meta` fields to a GraphQL object.
fn with_document_fields(fields: Object, schema: &Schema) -> Object {
    fields
        // The `fields` field passes down the parent value to its children
        .field(
            Field::new(
                constants::FIELDS_FIELD,
                TypeRef::named(fields_name(schema.id())),
                move |ctx| {
                    FieldFuture::new(async move {
                        // Downcast the document which has already been retrieved from the store by
                        // the root query resolver and passed down to the `fields` field here
                        let document = Resolved::downcast(&ctx);

                        // We continue to pass it down to all the fields' children
                        Ok(Some(FieldValue::owned_any(document)))
                    })
                },
            )
            .description(format!(
                "Application fields of a `{}` document.",
                schema.id().name()
            )),
        )
        // The `meta` field of a document, resolves the `DocumentMeta` object
        .field(
            Field::new(
                constants::META_FIELD,
                TypeRef::named(constants::DOCUMENT_META),
                move |ctx| FieldFuture::new(async move { resolve_document_meta(ctx).await }),
            )
            .description(format!(
                "Meta fields of a `{}` document.",
                schema.id().name()
            )),
        )
        .description(schema.description().to_string())
}
