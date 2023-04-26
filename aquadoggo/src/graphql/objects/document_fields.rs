// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object};
use p2panda_rs::schema::{FieldType, Schema};

use crate::graphql::resolvers::resolve_document_field;
use crate::graphql::utils::{fields_name, graphql_type, with_collection_arguments};

/// Dynamically build GraphQL objects describing the application fields of a p2panda schema.
///
/// Each generated object has a type name with the formatting `<schema_id>Fields`.
pub fn build_document_fields_object(schema: &Schema) -> Object {
    // Construct the document fields object which will be named `<schema_id>Fields`
    let schema_field_name = fields_name(schema.id());
    let mut document_schema_fields = Object::new(&schema_field_name);

    // For every field in the schema we create a type with a resolver
    for (name, field_type) in schema.fields().iter() {
        // If this is a relation list type we add an argument for filtering items in the list
        let field = match field_type {
            FieldType::RelationList(schema_id) | FieldType::PinnedRelationList(schema_id) => {
                with_collection_arguments(
                    Field::new(name, graphql_type(field_type), move |ctx| {
                        FieldFuture::new(async move { resolve_document_field(ctx).await })
                    }),
                    schema_id,
                )
            }
            _ => Field::new(name, graphql_type(field_type), move |ctx| {
                FieldFuture::new(async move { resolve_document_field(ctx).await })
            })
            .description(format!(
                "The `{}` field of a {} document.",
                name,
                schema.id().name()
            )),
        };
        document_schema_fields = document_schema_fields.field(field).description(format!(
            "The application fields of a `{}` document.",
            schema.id().name()
        ));
    }

    document_schema_fields
}
