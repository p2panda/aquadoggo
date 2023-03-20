// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldName, FieldType, Schema};

use crate::db::errors::QueryError;
use crate::db::query::{Field, Filter, FilterBy, FilterItem, MetaField, Order};

fn validate_type(
    field_name: &str,
    query_field: &OperationValue,
    schema_field: &FieldType,
) -> Result<(), QueryError> {
    if query_field.field_type() != schema_field.to_string() {
        return Err(QueryError::FilterInvalidType(
            query_field.field_type().to_string(),
            field_name.to_string(),
            schema_field.to_string(),
        ));
    }

    Ok(())
}

pub fn validate_query(filter: &Filter, order: &Order, schema: &Schema) -> Result<(), QueryError> {
    let schema_fields: HashMap<&FieldName, &FieldType> = schema.fields().iter().collect();

    // Make sure field to order actually exists in schema
    match &order.field {
        Field::Meta(_) => {
            // Ordering any meta field is always okay
        }
        Field::Field(field_name) => {
            if !schema_fields.contains_key(field_name) {
                return Err(QueryError::OrderFieldUnknown(field_name.clone()));
            }
        }
    };

    // Make sure field to filter exists and filtering value is of correct type
    for field in filter.iter() {
        match &field.field {
            // Filtering a "meta" field
            Field::Meta(meta_field) => match (&field.by, meta_field) {
                // Make sure that "documentId" filter values are relation (strings)
                (FilterBy::Element(element), MetaField::DocumentId) => {
                    validate_type(
                        &meta_field.to_string(),
                        element,
                        &FieldType::Relation(schema.id().clone()),
                    )?;
                }

                // Make sure that "viewId" filter values are pinned relation (strings)
                (FilterBy::Element(element), MetaField::DocumentViewId) => {
                    validate_type(
                        &meta_field.to_string(),
                        element,
                        &FieldType::PinnedRelation(schema.id().clone()),
                    )?;
                }

                // Make sure that "owner" filter values are strings
                (FilterBy::Element(element), MetaField::Owner) => {
                    validate_type(&meta_field.to_string(), element, &FieldType::String)?;
                }

                // Make sure that "edited" and "deleted" filter values are boolean
                (FilterBy::Element(element), MetaField::Edited)
                | (FilterBy::Element(element), MetaField::Deleted) => {
                    validate_type(&meta_field.to_string(), element, &FieldType::Boolean)?;
                }

                // Filtering over multiple values of "edited" and "deleted" flag is not permitted,
                // for all other meta fields this is okay
                (FilterBy::Set(_), MetaField::Edited) | (FilterBy::Set(_), MetaField::Deleted) => {
                    return Err(QueryError::FilterInvalidSet(meta_field.to_string()));
                }

                // Filtering over an interval for meta fields is not permitted
                (FilterBy::Interval(_, _), _) => {
                    return Err(QueryError::FilterInvalidInterval(meta_field.to_string()))
                }

                // Text search for meta fields is not permitted
                (FilterBy::Contains(_), _) => {
                    return Err(QueryError::FilterInvalidSearch(meta_field.to_string()))
                }

                _ => (),
            },

            // Filtering a "fields" field (application data)
            Field::Field(field_name) => {
                // Make sure filtered field actually exists in schema
                if !schema_fields.contains_key(field_name) {
                    return Err(QueryError::FilterFieldUnknown(field_name.clone()));
                }

                match (&field.by, *schema_fields.get(field_name).unwrap()) {
                    // Check if element filter value matches schema type
                    (FilterBy::Element(element), schema_field_type) => {
                        validate_type(field_name, element, schema_field_type)?;
                    }

                    // Check if all elements in set filter match schema type
                    (FilterBy::Set(elements), schema_field_type) => {
                        if schema_field_type == &FieldType::Boolean {
                            return Err(QueryError::FilterInvalidSet(field_name.to_string()));
                        }

                        for element in elements {
                            validate_type(field_name, element, schema_field_type)?;
                        }
                    }

                    // Disallow interval filters for booleans and relations
                    (FilterBy::Interval(_, _), FieldType::Boolean)
                    | (FilterBy::Interval(_, _), FieldType::Relation(_))
                    | (FilterBy::Interval(_, _), FieldType::RelationList(_))
                    | (FilterBy::Interval(_, _), FieldType::PinnedRelation(_))
                    | (FilterBy::Interval(_, _), FieldType::PinnedRelationList(_)) => {
                        return Err(QueryError::FilterInvalidInterval(field_name.to_string()))
                    }

                    // Disallow search filters for everything which is not a free-form string
                    (FilterBy::Contains(_), FieldType::Boolean)
                    | (FilterBy::Contains(_), FieldType::Integer)
                    | (FilterBy::Contains(_), FieldType::Float)
                    | (FilterBy::Contains(_), FieldType::Relation(_))
                    | (FilterBy::Contains(_), FieldType::RelationList(_))
                    | (FilterBy::Contains(_), FieldType::PinnedRelation(_))
                    | (FilterBy::Contains(_), FieldType::PinnedRelationList(_)) => {
                        return Err(QueryError::FilterInvalidSearch(field_name.to_string()))
                    }

                    _ => (),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // @TODO
    }
}
