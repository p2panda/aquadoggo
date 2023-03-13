// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, ResolverContext, TypeRef};
use async_graphql::{Error, Result};
use async_recursion::async_recursion;
use dynamic_graphql::FieldValue;
use futures::future;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::types::StorageDocument;
use crate::db::SqlStore;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::{
    downcast_id_params, fields_name, get_document_from_params, gql_scalar,
};
use crate::schema::SchemaProvider;

/// Get the GraphQL type name for a p2panda field type.
///
/// GraphQL types for relations use the p2panda schema id as their name.
fn graphql_type(field_type: &FieldType) -> TypeRef {
    match field_type {
        p2panda_rs::schema::FieldType::Boolean => TypeRef::named_nn(TypeRef::BOOLEAN),
        p2panda_rs::schema::FieldType::Integer => TypeRef::named_nn(TypeRef::INT),
        p2panda_rs::schema::FieldType::Float => TypeRef::named_nn(TypeRef::FLOAT),
        p2panda_rs::schema::FieldType::String => TypeRef::named_nn(TypeRef::STRING),
        p2panda_rs::schema::FieldType::Relation(schema_id) => TypeRef::named(schema_id.to_string()),
        p2panda_rs::schema::FieldType::RelationList(schema_id) => {
            TypeRef::named_list(schema_id.to_string())
        }
        p2panda_rs::schema::FieldType::PinnedRelation(schema_id) => {
            TypeRef::named(schema_id.to_string())
        }
        p2panda_rs::schema::FieldType::PinnedRelationList(schema_id) => {
            TypeRef::named_list(schema_id.to_string())
        }
    }
}

pub fn build_document_field_schema(
    document_fields: Object,
    name: String,
    field_type: &FieldType,
    document_id: Option<DocumentIdScalar>,
    document_view_id: Option<DocumentViewIdScalar>,
) -> Object {
    // The type of this field.
    let field_type = field_type.clone();
    let graphql_type = graphql_type(&field_type);

    // Define the field and create a resolver.
    document_fields.field(Field::new(name.clone(), graphql_type, move |ctx| {
        let store = ctx.data_unchecked::<SqlStore>();
        let document_id = document_id.clone();
        let document_view_id = document_view_id.clone();
        let field_type = field_type.clone();
        let name = name.clone();

        FieldFuture::new(async move {
            match field_type {
                FieldType::Relation(schema_id) => {
                    let document = get_document_from_params(store, &document_id, &document_view_id).await?;
                    let field = match document {
                        Some(document) => document.get(&name).unwrap().clone(),
                        None => return Ok(FieldValue::NONE),
                    };
                    let document_id = match field {
                        OperationValue::Relation(rel) => rel.document_id().into(),
                        _ => panic!()
                    };
                    match build_relation_field(ctx, schema_id, Some(document_id), None)
                        .await?
                    {
                        Some(value) => Ok(Some(FieldValue::owned_any(value))),
                        None => Ok(FieldValue::NONE),
                    }
                }
                FieldType::RelationList(schema_id) => todo!(),
                FieldType::PinnedRelation(schema_id) => {
                    let document = get_document_from_params(store, &document_id, &document_view_id).await?;
                    let field = match document {
                        Some(document) => document.get(&name).unwrap().clone(),
                        None => return Ok(FieldValue::NONE),
                    };
                    let document_view_id = match field {
                        OperationValue::PinnedRelation(rel) => rel.view_id().into(),
                        _ => panic!()
                    };
                    match build_relation_field(ctx, schema_id, None, Some(document_view_id))
                        .await?
                    {
                        Some(value) => Ok(Some(FieldValue::owned_any(value))),
                        None => Ok(FieldValue::NONE),
                    }
                }
                FieldType::PinnedRelationList(_) => todo!(),
                _ => resolve_field(ctx, document_id, document_view_id).await,
            }
        })
    }))
}

async fn resolve_field<'a>(
    ctx: ResolverContext<'a>,
    document_id: Option<DocumentIdScalar>,
    document_view_id: Option<DocumentViewIdScalar>,
) -> Result<Option<FieldValue>, Error> {
    let store = ctx.data_unchecked::<SqlStore>();
    let field_name = ctx.field().name();

    let (document_id, document_view_id) = if document_id.is_none() && document_view_id.is_none() {
        downcast_id_params(&ctx)
    } else {
        (document_id, document_view_id)
    };

    // Get the whole document.
    //
    // TODO: This can be optimized with per field SQL queries and data loader.
    let document = get_document_from_params(store, &document_id, &document_view_id).await?;

    match document {
        Some(document) => {
            let value = document
                .get(field_name)
                .expect("Only fields defined on the schema can be queried");

            Ok(Some(FieldValue::value(gql_scalar(value))))
        }
        None => Ok(FieldValue::NONE),
    }
}

async fn build_relation_field<'a>(
    ctx: ResolverContext<'a>,
    schema_id: SchemaId,
    document_id: Option<DocumentIdScalar>,
    document_view_id: Option<DocumentViewIdScalar>,
) -> Result<Option<Object>, Error> {
    let schema_provider = ctx.data_unchecked::<SchemaProvider>();
    match schema_provider.get(&schema_id).await {
        Some(schema) => {
            // Construct the document fields object which will be named `<schema_id>Field`.
            let schema_field_name = fields_name(&schema.id().to_string());
            let mut document_schema_fields = Object::new(&schema_field_name);

            // For every field in the schema we create a type with a resolver.
            //
            // TODO: We can optimize the field resolution methods later with a data loader.
            for (name, field_type) in schema.fields() {
                document_schema_fields = build_document_field_schema(
                    document_schema_fields,
                    name.to_string(),
                    field_type,
                    document_id.clone(),
                    document_view_id.clone(),
                );
            }
            Ok(Some(document_schema_fields))
        }
        None => todo!(),
    }
}
