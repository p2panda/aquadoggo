// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{Field, FieldFuture, Object, TypeRef};
use dynamic_graphql::FieldValue;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::FieldType;

use crate::db::SqlStore;
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::utils::{downcast_id_params, get_document_from_params, gql_scalar};

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
) -> Object {
    // The type of this field.
    let field_type = field_type.clone();
    let graphql_type = graphql_type(&field_type);

    // Define the field and create a resolver.
    document_fields.field(Field::new(name.clone(), graphql_type, move |ctx| {
        let store = ctx.data_unchecked::<SqlStore>();
        let name = name.clone();

        FieldFuture::new(async move {
            // Parse the bubble up message.
            let (document_id, document_view_id) = downcast_id_params(&ctx);

            // Get the whole document from the store.
            //
            // TODO: This can be optimized by using a data loader.
            let document =
                match get_document_from_params(store, &document_id, &document_view_id).await? {
                    Some(document) => document,
                    None => return Ok(FieldValue::NONE),
                };

            // Get the field this query is concerned with.
            match document.get(&name).unwrap() {
                // Relation fields are expected to resolve to the related document so we pass
                // along the document id which will be processed through it's own resolver.
                OperationValue::Relation(rel) => Ok(Some(FieldValue::owned_any((
                    Some(DocumentIdScalar::from(rel.document_id())),
                    None::<DocumentViewIdScalar>,
                )))),
                // Relation lists are handled by collecting and returning a list of all document
                // id's in the relation list. Each of these in turn are processed and queries
                // forwarded up the tree via their own respective resolvers.
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
                // Pinned relation behaves the same as relation but passes along a document view id.
                OperationValue::PinnedRelation(rel) => Ok(Some(FieldValue::owned_any((
                    None::<DocumentIdScalar>,
                    Some(DocumentViewIdScalar::from(rel.view_id())),
                )))),
                // Pinned relation lists behave the same as relation lists but pass along view ids.
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
                // All other fields are simply resolved to their scalar value.
                value => Ok(Some(FieldValue::value(gql_scalar(value)))),
            }
        })
    }))
}
