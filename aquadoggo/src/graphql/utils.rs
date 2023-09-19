// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};
use std::num::NonZeroU64;

use async_graphql::dynamic::{InputValue, ObjectAccessor, ResolverContext, TypeRef, ValueAccessor};
use async_graphql::{Error, Value};
use dynamic_graphql::ScalarValue;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::db::query::{
    Direction, Field, Filter, MetaField, Order, Pagination, PaginationField, Select,
};
use crate::db::stores::{PaginationCursor, Query, RelationList};
use crate::db::types::StorageDocument;
use crate::db::SqlStore;
use crate::graphql::constants;
use crate::graphql::scalars::{CursorScalar, DocumentIdScalar, DocumentViewIdScalar};

use super::scalars::HexBytesScalar;

// Type name suffixes.
const DOCUMENT_FIELDS_SUFFIX: &str = "Fields";
const DOCUMENT_FIELDS_INPUT_SUFFIX: &str = "FieldsInput";
const FILTER_INPUT_SUFFIX: &str = "Filter";
const ORDER_BY_SUFFIX: &str = "OrderBy";
const COLLECTION_ITEM_SUFFIX: &str = "Item";
const COLLECTION_SUFFIX: &str = "Collection";

/// Formats the name of a document collection type.
pub fn collection_name(schema_id: &SchemaId) -> String {
    format!("{}{COLLECTION_SUFFIX}", schema_id)
}

/// Formats the name of a collection item type.
pub fn collection_item_name(schema_id: &SchemaId) -> String {
    format!("{}{COLLECTION_ITEM_SUFFIX}", schema_id)
}

/// Formats the name of a document fields type.
pub fn fields_name(schema_id: &SchemaId) -> String {
    format!("{}{DOCUMENT_FIELDS_SUFFIX}", schema_id)
}

/// Formats the name of a document fields input type.
pub fn fields_input_name(schema_id: &SchemaId) -> String {
    format!("{}{DOCUMENT_FIELDS_INPUT_SUFFIX}", schema_id)
}

/// Formats the name of a collection filter type.
pub fn filter_name(schema_id: &SchemaId) -> String {
    format!("{}{FILTER_INPUT_SUFFIX}", schema_id)
}

/// Formats the name of an order by type.
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
        OperationValue::Bytes(value) => {
            let hex_string = hex::encode(value);
            hex_string.into()
        }
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
        FieldType::Bytes => TypeRef::named("HexBytes"),
        FieldType::Relation(schema_id) => TypeRef::named(schema_id.to_string()),
        FieldType::RelationList(schema_id) => TypeRef::named(collection_name(schema_id)),
        FieldType::PinnedRelation(schema_id) => TypeRef::named(schema_id.to_string()),
        FieldType::PinnedRelationList(schema_id) => TypeRef::named(collection_name(schema_id)),
    }
}

/// Convert a schema field to a GraphQL input.
pub fn graphql_input(field_type: &FieldType) -> TypeRef {
    match field_type {
        FieldType::Boolean => TypeRef::named(TypeRef::BOOLEAN),
        FieldType::Integer => TypeRef::named(TypeRef::INT),
        FieldType::Float => TypeRef::named(TypeRef::FLOAT),
        FieldType::String => TypeRef::named(TypeRef::STRING),
        FieldType::Bytes => TypeRef::named("HexBytes"),
        FieldType::Relation(_) => TypeRef::named("DocumentId"),
        FieldType::PinnedRelation(_) => TypeRef::named("DocumentViewId"),
        FieldType::RelationList(_) => TypeRef::named_list("DocumentId"),
        FieldType::PinnedRelationList(_) => TypeRef::named_list("DocumentViewId"),
    }
}

/// Convert a async_graphql value into an operation value.
pub fn graphql_to_operation_value(value: &Value, field_type: FieldType) -> OperationValue {
    match field_type {
        FieldType::Boolean => {
            if let Value::Boolean(value) = value {
                value.to_owned().into()
            } else {
                unreachable!()
            }
        }
        FieldType::Integer => {
            if let Value::Number(value) = value {
                value.as_i64().expect("Value is integer").into()
            } else {
                unreachable!()
            }
        }
        FieldType::Float => {
            if let Value::Number(value) = value {
                value.as_f64().expect("Value is integer").into()
            } else {
                unreachable!()
            }
        }
        FieldType::String => {
            if let Value::String(value) = value {
                value.to_owned().into()
            } else {
                unreachable!()
            }
        }
        FieldType::Bytes => {
            let hex_bytes: String = HexBytesScalar::from_value(value.to_owned())
                .expect("Value is hex byte string")
                .into();
            OperationValue::Bytes(hex::decode(hex_bytes).expect("Valid hex byte string"))
        }
        FieldType::Relation(_) => {
            let document_id: DocumentId = DocumentIdScalar::from_value(value.to_owned())
                .expect("Value is document id")
                .into();
            document_id.into()
        }
        FieldType::PinnedRelation(_) => {
            let document_view_id: DocumentViewId =
                DocumentViewIdScalar::from_value(value.to_owned())
                    .expect("Value is document id")
                    .into();
            document_view_id.into()
        }
        FieldType::RelationList(_) => {
            if let Value::List(value_list) = value {
                let document_ids: Vec<DocumentId> = value_list
                    .iter()
                    .map(|value| {
                        DocumentIdScalar::from_value(value.to_owned())
                            .expect("Value is document id")
                            .into()
                    })
                    .collect();
                document_ids.into()
            } else {
                unreachable!()
            }
        }
        FieldType::PinnedRelationList(_) => {
            if let Value::List(value_list) = value {
                let document_ids: Vec<DocumentViewId> = value_list
                    .iter()
                    .map(|value| {
                        DocumentViewIdScalar::from_value(value.to_owned())
                            .expect("Value is document view id")
                            .into()
                    })
                    .collect();
                document_ids.into()
            } else {
                unreachable!()
            }
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
        FieldType::Bytes => {
            let hex_string = filter_value.string()?;
            let bytes = hex::decode(hex_string)?;
            bytes[..].into()
        }
        // We are only ever dealing with list items here
        FieldType::Relation(_) | FieldType::RelationList(_) => {
            DocumentId::new(&filter_value.string()?.parse()?).into()
        }
        FieldType::PinnedRelation(_) | FieldType::PinnedRelationList(_) => {
            let document_view_id: DocumentViewId = filter_value.string()?.parse()?;
            document_view_id.into()
        }
    };

    Ok(value)
}

/// Parse all argument values based on expected keys and types.
pub fn parse_collection_arguments(
    ctx: &ResolverContext,
    schema: &Schema,
    list: &Option<RelationList>,
) -> Result<Query<PaginationCursor>, Error> {
    let mut pagination = Pagination::<PaginationCursor>::default();
    let mut order = Order::default();
    let mut filter = Filter::default();

    for (name, value) in ctx.args.iter() {
        match name.as_str() {
            constants::PAGINATION_AFTER_ARG => {
                let cursor: CursorScalar = value.string()?.parse()?;
                pagination.after = Some(cursor.into());
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
                order.field = Some(order_by);
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
                parse_meta_filter(&mut filter, &filter_object)?;
            }
            constants::FILTER_ARG => {
                let filter_object = value
                    .object()
                    .map_err(|_| Error::new("internal: is not an object"))?;
                parse_filter(&mut filter, schema, &filter_object)?;
            }
            _ => panic!("Unknown argument key received"),
        }
    }

    // Parse selected fields in GraphQL query
    let (pagination_fields, fields) = look_ahead_selected_fields(ctx);
    let select = Select::new(fields.as_slice());
    pagination.fields = pagination_fields;

    // Set default ordering to document id as per specification, if we're in a root query
    if list.is_none() && order.field.is_none() {
        order.field = Some(Field::Meta(MetaField::DocumentId));
    }

    // Finally put it all together
    let query = Query::new(&pagination, &select, &filter, &order);

    Ok(query)
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
                    filter.add_contains(&filter_field, value.string()?);
                }
                "notContains" => {
                    filter.add_not_contains(&filter_field, value.string()?);
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
            let field_type = match field.as_str() {
                "edited" | "deleted" => FieldType::Boolean,
                _ => FieldType::String,
            };
            match name.as_str() {
                "in" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &field_type)?;
                        list_items.push(item);
                    }
                    filter.add_in(&filter_field, &list_items);
                }
                "notIn" => {
                    let mut list_items: Vec<OperationValue> = vec![];
                    for value in value.list()?.iter() {
                        let item = filter_to_operation_value(&value, &field_type)?;
                        list_items.push(item);
                    }
                    filter.add_not_in(&filter_field, &list_items);
                }
                "eq" => {
                    let value = filter_to_operation_value(&value, &field_type)?;
                    filter.add(&filter_field, &value);
                }
                "notEq" => {
                    let value = filter_to_operation_value(&value, &field_type)?;
                    filter.add_not(&filter_field, &value);
                }
                _ => panic!("Unknown meta filter type received"),
            }
        }
    }
    Ok(())
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
                .get_document_by_view_id(&document_view_id.clone().into())
                .await
        }
        (Some(document_id), None) => store.get_document(&document_id.clone().into()).await,
        _ => panic!("Invalid values passed from query field parent"),
    }
}

/// Add collection query arguments to a field.
pub fn with_collection_arguments(
    field: async_graphql::dynamic::Field,
    schema_id: &SchemaId,
) -> async_graphql::dynamic::Field {
    field
        .argument(
            InputValue::new(
                constants::FILTER_ARG,
                TypeRef::named(filter_name(schema_id)),
            )
            .description("Filter the query based on field values"),
        )
        .argument(
            InputValue::new(
                constants::META_FILTER_ARG,
                TypeRef::named("MetaFilterInputObject"),
            )
            .description("Filter the query based on meta field values"),
        )
        .argument(
            InputValue::new(
                constants::ORDER_BY_ARG,
                TypeRef::named(order_by_name(schema_id)),
            )
            .description("Field by which items in the collection will be ordered")
            .default_value("DOCUMENT_ID"),
        )
        .argument(
            InputValue::new(
                constants::ORDER_DIRECTION_ARG,
                TypeRef::named("OrderDirection"),
            )
            .description("Direction which items in the collection will be ordered")
            .default_value("ASC"),
        )
        .argument(
            InputValue::new(
                constants::PAGINATION_FIRST_ARG,
                TypeRef::named(TypeRef::INT),
            )
            .description("Number of paginated items we want from this request")
            .default_value(25),
        )
        .argument(
            InputValue::new(constants::PAGINATION_AFTER_ARG, TypeRef::named("Cursor"))
                .description("The item we wish to start paginating from identified by a cursor"),
        )
        .description(format!(
            "Get all {} documents with pagination, ordering and filtering.",
            schema_id
        ))
}

/// Helper method to extract selected pagination and application fields from query.
pub fn look_ahead_selected_fields(ctx: &ResolverContext) -> (Vec<PaginationField>, Vec<Field>) {
    let selection_field = ctx
        .look_ahead()
        .selection_fields()
        .first()
        .expect("Needs always root selection field")
        .to_owned();

    let pagination = selection_field
        .selection_set()
        .filter_map(|field| match field.name() {
            // Remove special GraphQL meta fields
            "__typename" => None,

            // Remove all other fields which are not related to pagination
            constants::DOCUMENTS_FIELD => None,

            // Convert pagination fields finally
            value => Some(value.into()),
        })
        .collect::<Vec<PaginationField>>();

    let mut selected_fields = Vec::new();

    if let Some(document) = selection_field
        .selection_set()
        .find(|field| field.name() == constants::DOCUMENTS_FIELD)
    {
        document
            .selection_set()
            .for_each(|field| match field.name() {
                // Parse selected application fields
                constants::FIELDS_FIELD => {
                    field.selection_set().for_each(|field| match field.name() {
                        // Remove special GraphQL meta fields
                        "__typename" => (),
                        field_name => {
                            selected_fields.push(Field::Field(field_name.to_string()));
                        }
                    });
                }
                // Parse selected meta fields
                constants::META_FIELD => {
                    field
                        .selection_set()
                        .filter_map(|field| field.name().try_into().ok())
                        .for_each(|field| {
                            selected_fields.push(Field::Meta(field));
                        });
                }
                _ => (),
            });
    }

    (pagination, selected_fields)
}
