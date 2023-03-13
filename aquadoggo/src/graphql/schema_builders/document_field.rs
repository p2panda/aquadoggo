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
        let name = name.clone();

        FieldFuture::new(async move {
            let (document_id, document_view_id) =
                if document_id.is_none() && document_view_id.is_none() {
                    downcast_id_params(&ctx)
                } else {
                    (document_id, document_view_id)
                };

            let document =
                match get_document_from_params(store, &document_id, &document_view_id).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

            match document.get(&name).unwrap() {
                OperationValue::Relation(rel) => Ok(Some(FieldValue::owned_any((
                    Some(DocumentIdScalar::from(rel.document_id())),
                    None::<DocumentViewIdScalar>,
                )))),
                OperationValue::RelationList(rel) => {
                    let mut fields = vec![];
                    for document_id in rel.iter() {
                        fields.push(FieldValue::owned_any((
                            Some(DocumentIdScalar::from(document_id)),
                            None::<DocumentViewIdScalar>,
                        )));
                    }
                    Ok(Some(FieldValue::list(fields)))
                }
                OperationValue::PinnedRelation(rel) => Ok(Some(FieldValue::owned_any((
                    None::<DocumentIdScalar>,
                    Some(DocumentViewIdScalar::from(rel.view_id())),
                )))),
                OperationValue::PinnedRelationList(rel) => {
                    let mut fields = vec![];
                    for document_view_id in rel.iter() {
                        fields.push(FieldValue::owned_any((
                            None::<DocumentIdScalar>,
                            Some(DocumentViewIdScalar::from(document_view_id)),
                        )));
                    }
                    Ok(Some(FieldValue::list(fields)))
                }
                value => Ok(Some(FieldValue::value(gql_scalar(value)))),
            }
        })
    }))
}
