// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::Value;
use async_graphql::dynamic::{Field, FieldFuture, Object, TypeRef};
use dynamic_graphql::FieldValue;
use futures::future;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::FieldType;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::SqlStore;
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
    name: &str,
    field_type: &FieldType,
) -> Object {
    // The type of this field.
    let graphql_type = graphql_type(field_type);

    // Define the field and create a resolver.
    document_fields.field(Field::new(name, graphql_type, move |ctx| {
        FieldFuture::new(async move {
            let store = ctx.data_unchecked::<SqlStore>();
            let field_name = ctx.field().name();

            // Downcast the parameters passed up from the parent query field.
            let (document_id, document_view_id) = downcast_id_params(&ctx);
            // Get the whole document.
            //
            // TODO: This can be optimized with per field SQL queries and data loader.
            let document = get_document_from_params(store, &document_id, &document_view_id).await?;

            match document {
                Some(document) => {
                    let value = document
                        .get(field_name)
                        .expect("Only fields defined on the schema can be queried");
                    // Convert the operation value into a graphql scalar type.
                    //
                    // TODO: Relation fields aren't supported yet, we need recursion.
                    let value = match value {
                        // Recurse into single views.
                        OperationValue::Relation(rel) => {
                            store.get_document(rel.document_id()).await?
                        }
                        OperationValue::PinnedRelation(rel) => {
                            store.get_document_by_view_id(rel.view_id()).await?
                        }

                        // Recurse into view lists.
                        OperationValue::RelationList(rel) => {
                            let queries = rel
                                .iter()
                                .map(|doc_id| store.get_document(doc_id));
                            Value::List(future::try_join_all(queries).await?)
                        }
                        OperationValue::PinnedRelationList(rel) => {
                            let queries = rel.iter().map(|view_id| {
                                store.get_document_by_view_id(view_id)
                            });
                            Value::List(future::try_join_all(queries).await?)
                        }

                        // Convert all simple fields to scalar values.
                        _ => gql_scalar(value),
                    };

                    Ok(Some(FieldValue::value(gql_scalar(value))))
                }
                None => Ok(Some(FieldValue::NULL)),
            }
        })
    }))
}
