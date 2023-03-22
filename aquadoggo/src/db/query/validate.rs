// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldName, FieldType, Schema};

use crate::db::query::errors::QueryError;
use crate::db::query::{Field, Filter, FilterBy, FilterItem, MetaField, Order, Select};

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

pub fn validate_query(
    select: &Select,
    filter: &Filter,
    order: &Order,
    schema: &Schema,
) -> Result<(), QueryError> {
    let schema_fields: HashMap<&FieldName, &FieldType> = schema.fields().iter().collect();

    // Make sure selected fields actually exists in schema
    for field in select.iter() {
        match field {
            Field::Meta(_) => {
                // Selecting any meta field is always okay
            }
            Field::Field(field_name) => {
                if !schema_fields.contains_key(field_name) {
                    return Err(QueryError::SelectFieldUnknown(field_name.clone()));
                }
            }
        }
    }

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
    use p2panda_rs::operation::OperationValue;
    use p2panda_rs::schema::Schema;
    use rstest::rstest;

    use crate::db::query::{Direction, Filter, Order, Select};
    use crate::test_utils::doggo_schema;

    use super::validate_query;

    #[rstest]
    #[case::defaults(Select::default(), Filter::default(), Order::default())]
    #[case::doggo_schema(
        Select::new(&[
            "username".into(),
            "height".into()
        ]),
        Filter::new().fields(&[
            ("username_not_in", &["bubu".into()]),
            ("height", &[20.5.into()]),
            ("age_gte", &[16.into()]),
            ("is_admin_not", &[true.into()]),
        ]),
        Order::new(
            &"username".into(),
            &Direction::Descending
        )
    )]
    fn valid_queries(#[case] select: Select, #[case] filter: Filter, #[case] order: Order) {
        if let Err(err) = validate_query(&select, &filter, &order, &doggo_schema()) {
            panic!("{}", err)
        }
    }

    #[rstest]
    #[case::select_unknown_field(
        Select::new(&[
            "message".into(),
        ]),
        Filter::default(),
        Order::default(),
        "Can't select unknown field 'message'"
    )]
    #[case::filter_unknown_field(
        Select::default(),
        Filter::new().fields(&[("message_contains", &["test".into()])]),
        Order::default(),
        "Can't apply filter on unknown field 'message'"
    )]
    #[case::order_unknown_field(
        Select::default(),
        Filter::default(),
        Order::new(&"message".into(), &Direction::Ascending),
        "Can't apply ordering on unknown field 'message'"
    )]
    #[case::invalid_meta_field_type(
        Select::default(),
        Filter::new().meta_fields(&[
            ("documentId".into(), &["test".into()])
        ]),
        Order::default(),
        "Filter type 'str' for field 'documentId' is not matching schema type 'relation(doggo_schema_0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543)'"
    )]
    #[case::invalid_field_type(
        Select::default(),
        Filter::new().fields(&[
            ("username".into(), &[2020.into()])
        ]),
        Order::default(),
        "Filter type 'int' for field 'username' is not matching schema type 'str'"
    )]
    #[case::invalid_interval(
        Select::default(),
        Filter::new().fields(&[
            ("is_admin_in".into(), &[true.into(), false.into()])
        ]),
        Order::default(),
        "Can't apply set filter as field 'is_admin' is of type boolean"
    )]
    #[case::invalid_search(
        Select::default(),
        Filter::new().fields(&[
            ("age_contains".into(), &[22.into()])
        ]),
        Order::default(),
        "Can't apply search filter as field 'age' is not of type string"
    )]
    #[case::invalid_set_types(
        Select::default(),
        Filter::new().fields(&[
            ("username_in".into(), &["bubu".into(), 2020.into()])
        ]),
        Order::default(),
        "Filter type 'int' for field 'username' is not matching schema type 'str'"
    )]
    fn invalid_queries(
        #[case] select: Select,
        #[case] filter: Filter,
        #[case] order: Order,
        #[case] expected: &str,
    ) {
        assert_eq!(
            validate_query(&select, &filter, &order, &doggo_schema())
                .expect_err("Expect error")
                .to_string(),
            expected
        );
    }
}
