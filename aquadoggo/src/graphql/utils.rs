// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::dynamic::{ResolverContext, TypeRef, ValueAccessor};
use async_graphql::{Error, Value};
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::{OperationValue, Relation};
use p2panda_rs::schema::{FieldType, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::{types::StorageDocument, SqlStore};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::types::DocumentValue;

const DOCUMENT_FIELDS_SUFFIX: &str = "Fields";
const FILTER_INPUT_SUFFIX: &str = "Filter";
const ORDER_BY_SUFFIX: &str = "OrderBy";
const PAGINATED_DOCUMENT_SUFFIX: &str = "Paginated";
const PAGINATED_RESPONSE_SUFFIX: &str = "PaginatedResponse";

// Correctly formats the name of a paginated response type.
pub fn paginated_response_name(schema_id: &SchemaId) -> String {
    format!("{}{PAGINATED_RESPONSE_SUFFIX}", schema_id)
}

// Correctly formats the name of a paginated document type.
pub fn paginated_document_name(schema_id: &SchemaId) -> String {
    format!("{}{PAGINATED_DOCUMENT_SUFFIX}", schema_id)
}

// Correctly formats the name of a document field type.
pub fn fields_name(schema_id: &SchemaId) -> String {
    format!("{}{DOCUMENT_FIELDS_SUFFIX}", schema_id)
}

// Correctly formats the name of a document filter type.
pub fn filter_name(schema_id: &SchemaId) -> String {
    format!("{}{FILTER_INPUT_SUFFIX}", schema_id)
}

// Correctly formats the name of an order by type.
pub fn order_by_name(schema_id: &SchemaId) -> String {
    format!("{}{ORDER_BY_SUFFIX}", schema_id)
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
        FieldType::Boolean => TypeRef::named(TypeRef::BOOLEAN),
        FieldType::Integer => TypeRef::named(TypeRef::INT),
        FieldType::Float => TypeRef::named(TypeRef::FLOAT),
        FieldType::String => TypeRef::named(TypeRef::STRING),
        FieldType::Relation(schema_id) => TypeRef::named(schema_id.to_string()),
        FieldType::RelationList(schema_id) => {
            TypeRef::named_list(paginated_response_name(schema_id))
        }
        FieldType::PinnedRelation(schema_id) => TypeRef::named(schema_id.to_string()),
        FieldType::PinnedRelationList(schema_id) => {
            TypeRef::named_list(paginated_response_name(schema_id))
        }
    }
}

/// Parse a filter value into a typed operation value.
pub fn filter_to_operation_value(
    filter_value: &ValueAccessor,
    field_type: &FieldType,
) -> Result<OperationValue, Error> {
    let value = match field_type {
        FieldType::Boolean => filter_value.boolean()?.into(),
        FieldType::Integer => filter_value.i64()?.into(),
        FieldType::Float => filter_value.f64()?.into(),
        FieldType::String => filter_value.string()?.into(),
        FieldType::Relation(_) => DocumentId::new(&filter_value.string()?.parse()?).into(),
        FieldType::RelationList(_) => {
            let mut list_items = vec![];
            for value in filter_value.list()?.iter() {
                list_items.push(DocumentId::new(&value.string()?.parse()?));
            }
            list_items.into()
        }
        FieldType::PinnedRelation(_) => {
            let document_view_id: DocumentViewId = filter_value.string()?.parse()?;
            document_view_id.into()
        }
        FieldType::PinnedRelationList(_) => {
            let mut list_items = vec![];
            for value in filter_value.list()?.iter() {
                let document_view_id: DocumentViewId = value.string()?.parse()?;
                list_items.push(document_view_id);
            }
            list_items.into()
        }
    };

    Ok(value)
}

/// Downcast document value which will have been passed up by the parent query node,
/// retrieved via the `ResolverContext`.
///
/// We unwrap internally here as we expect validation to have occured in the query resolver.
pub fn downcast_document(ctx: &ResolverContext) -> DocumentValue {
    ctx.parent_value
        .downcast_ref::<DocumentValue>()
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
