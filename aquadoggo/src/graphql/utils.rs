// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::num::NonZeroU64;

use async_graphql::dynamic::{ObjectAccessor, ResolverContext, TypeRef, ValueAccessor};
use async_graphql::{Error, Value};
use dynamic_graphql::ScalarValue;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination};
use crate::db::{types::StorageDocument, SqlStore};
use crate::graphql::scalars::{DocumentIdScalar, DocumentViewIdScalar};
use crate::graphql::types::DocumentValue;

use super::constants;
use super::scalars::CursorScalar;

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

/// Parse all argument values based on expected keys and types.
pub fn parse_collection_arguments(
    ctx: &ResolverContext,
    schema: &Schema,
    pagination: &mut Pagination<CursorScalar>,
    order: &mut Order,
    filter: &mut Filter,
) -> Result<(), Error> {
    for (name, value) in ctx.args.iter() {
        match name.as_str() {
            constants::PAGINATION_CURSOR_ARG => {
                let cursor = CursorScalar::from_value(Value::String(value.string()?.to_string()))?;
                pagination.after = Some(cursor);
            }
            constants::PAGINATION_FIRST_ARG => {
                pagination.first = NonZeroU64::try_from(value.u64()?)?;
            }
            constants::ORDER_BY_ARG => {
                let order_by = match value.enum_name()? {
                    "OWNER" => Field::Meta(MetaField::Owner),
                    "DOCUMENT_ID" => Field::Meta(MetaField::DocumentId),
                    "DOCUMENT_VIEW_ID" => Field::Meta(MetaField::DocumentViewId),
                    field_name => Field::new(field_name),
                };
                order.field = order_by;
            }
            constants::ORDER_DIRECTION_ARG => {
                let direction = match value.enum_name()? {
                    "ASC" => Direction::Ascending,
                    "DESC" => Direction::Descending,
                    _ => panic!("Unknown order direction argument key received"),
                };
                order.direction = direction;
            }
            constants::META_FILTER_ARG => {
                let filter_object = value
                    .object()
                    .map_err(|_| Error::new("internal: is not an object"))?;
                parse_meta_filter(filter, &filter_object)?;
            }
            constants::FILTER_ARG => {
                let filter_object = value
                    .object()
                    .map_err(|_| Error::new("internal: is not an object"))?;
                parse_filter(filter, &schema, &filter_object)?;
            }
            _ => panic!("Unknown argument key received"),
        }
    }
    Ok(())
}

/// Parse a filter object received from the graphql api into an abstract filter type based on the
/// schema of the documents being queried.
fn parse_filter(
    filter: &mut Filter,
    schema: &Schema,
    filter_object: &ObjectAccessor,
) -> Result<(), Error> {
    for (field, filters) in filter_object.iter() {
        let filter_field = Field::new(field.as_str());
        let filters = filters.object()?;
        for (name, value) in filters.iter() {
            let field_type = schema.fields().get(field.as_str()).unwrap();
            match name.as_str() {
                "in" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, field_type)?;
                        list_items.push(item);
                    }
                    filter.add_in(&filter_field, &list_items);
                }
                "notIn" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, field_type)?;
                        list_items.push(item);
                    }
                    filter.add_not_in(&filter_field, &list_items);
                }
                "eq" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add(&filter_field, &value);
                }
                "notEq" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_not(&filter_field, &value);
                }
                "gt" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_gt(&filter_field, &value);
                }
                "gte" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_gte(&filter_field, &value);
                }
                "lt" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_lt(&filter_field, &value);
                }
                "lte" => {
                    let value = filter_to_operation_value(&value, field_type)?;
                    filter.add_lte(&filter_field, &value);
                }
                "contains" => {
                    filter.add_contains(&filter_field, &value.string()?);
                }
                "notContains" => {
                    filter.add_contains(&filter_field, &value.string()?);
                }
                _ => panic!("Unknown filter type received"),
            }
        }
    }
    Ok(())
}

/// Parse a meta filter object received from the graphql api into an abstract filter type based on the
/// schema of the documents being queried.
fn parse_meta_filter(filter: &mut Filter, filter_object: &ObjectAccessor) -> Result<(), Error> {
    for (field, filters) in filter_object.iter() {
        let meta_field = MetaField::try_from(field.as_str())?;
        let filter_field = Field::Meta(meta_field);
        let filters = filters.object()?;
        for (name, value) in filters.iter() {
            match name.as_str() {
                "in" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &FieldType::String)?;
                        list_items.push(item);
                    }
                    filter.add_in(&filter_field, &list_items);
                }
                "notIn" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &FieldType::String)?;
                        list_items.push(item);
                    }
                    filter.add_not_in(&filter_field, &list_items);
                }
                "eq" => {
                    let field_type = match field.as_str() {
                        "edited" | "deleted" => FieldType::Boolean,
                        _ => FieldType::String,
                    };
                    let value = filter_to_operation_value(&value, &field_type)?;
                    filter.add(&filter_field, &value);
                }
                "notEq" => {
                    let field_type = match field.as_str() {
                        "edited" | "deleted" => FieldType::Boolean,
                        _ => FieldType::String,
                    };
                    let value = filter_to_operation_value(&value, &field_type)?;
                    filter.add_not(&filter_field, &value);
                }
                _ => panic!("Unknown meta filter type received"),
            }
        }
    }
    Ok(())
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
