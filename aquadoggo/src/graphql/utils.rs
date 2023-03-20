// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{ResolverContext, TypeRef};
use async_graphql::Value;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::{types::StorageDocument, SqlStore};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};

use super::scalars::PublicKeyScalar;

const DOCUMENT_FIELDS_SUFFIX: &str = "Fields";

// Correctly formats the name of a document field type.
pub fn fields_name(schema_id: &SchemaId) -> String {
    format!("{}{DOCUMENT_FIELDS_SUFFIX}", schema_id)
}

/// Convert non-relation operation values into GraphQL values.
///
/// Panics when given a relation field value.
pub fn gql_scalar(operation_value: &OperationValue) -> Value {
    match operation_value {
        OperationValue::Boolean(value) => value.to_owned().into(),
        OperationValue::Float(value) => value.to_owned().into(),
        OperationValue::Integer(value) => value.to_owned().into(),
        OperationValue::String(value) => value.to_owned().into(),
        _ => panic!("This method is not used for relation types"),
    }
}

/// Get the GraphQL type name for a p2panda field type.
///
/// GraphQL types for relations use the p2panda schema id as their name.
pub fn graphql_type(field_type: &FieldType) -> TypeRef {
    match field_type {
        p2panda_rs::schema::FieldType::Boolean => TypeRef::named(TypeRef::BOOLEAN),
        p2panda_rs::schema::FieldType::Integer => TypeRef::named(TypeRef::INT),
        p2panda_rs::schema::FieldType::Float => TypeRef::named(TypeRef::FLOAT),
        p2panda_rs::schema::FieldType::String => TypeRef::named(TypeRef::STRING),
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

/// Downcast document id and document view id from parameters passed up the query fields and
/// retrieved via the `ResolverContext`.
///
/// We unwrap internally here as we expect validation to have occured in the query resolver.
pub fn downcast_document_id_arguments(
    ctx: &ResolverContext,
) -> (Option<DocumentIdScalar>, Option<DocumentViewIdScalar>) {
    ctx.parent_value
        .downcast_ref::<(Option<DocumentIdScalar>, Option<DocumentViewIdScalar>)>()
        .expect("Values passed from query parent should match expected")
        .to_owned()
}

/// Downcast public key and document view id from parameters passed up the query fields and
/// retrieved via the `ResolverContext`.
/// 
/// We unwrap internally here as we expect validation to have occured in the query resolver.
pub fn downcast_next_args_arguments(
    ctx: &ResolverContext,
) -> (PublicKeyScalar, Option<DocumentViewIdScalar>) {
    ctx.parent_value
        .downcast_ref::<(PublicKeyScalar, Option<DocumentViewIdScalar>)>()
        .expect("Values passed from query parent should match expected")
        .to_owned()
}

/// Helper for getting a document from the store by either the document id or document view id.
pub async fn get_document_from_params(
    store: &SqlStore,
    document_id: &Option<DocumentIdScalar>,
    document_view_id: &Option<DocumentViewIdScalar>,
) -> Result<Option<StorageDocument>, DocumentStorageError> {
    match (document_id, document_view_id) {
        (None, Some(document_view_id)) => {
            store
                .get_document_by_view_id(&DocumentViewId::from(document_view_id.to_owned()))
                .await
        }
        (Some(document_id), None) => store.get_document(&DocumentId::from(document_id)).await,
        _ => panic!("Invalid values passed from query field parent"),
    }
}
