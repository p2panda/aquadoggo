// SPDX-License-Identifier: AGPL-3.0-or-later

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::prop_oneof;
use proptest::sample::select;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest_derive::Arbitrary;

use crate::proptests::schema_strategies::{SchemaFieldType, SchemaField};
use crate::proptests::utils::FieldName;

/// Possible values used in filter arguments. `UniqueIdentifier` is a placeholder for values which
/// can be derived at runtime in order to use identifiers which exist in on the node, these include
/// document and view ids and public keys. 
#[derive(Arbitrary, Debug, Clone)]
pub enum FilterValue {
    Boolean(bool),
    String(String),
    Integer(i64),
    Float(f64),
    UniqueIdentifier, // This is a placeholder for a document id, document view id or public key which is selected at testing time
}

/// Valid filter types, each containing a FilterValue.
#[derive(Arbitrary, Debug, Clone)]
pub enum Filter {
    Contains(FilterValue),
    NotContains(FilterValue),
    Equal(FilterValue),
    NotEqual(FilterValue),
    IsIn(FilterValue),
    NotIn(FilterValue),
    GreaterThan(FilterValue),
    LessThan(FilterValue),
    GreaterThanOrEqual(FilterValue),
    LessThanOrEqual(FilterValue),
}

/// Valid meta field types.
#[derive(Arbitrary, Debug, Clone)]
pub enum MetaField {
    Owner,
    DocumentId,
    DocumentViewId,
}

/// Top level strategy for generating meta field filters.
pub fn meta_field_filter_strategy() -> impl Strategy<Value = Vec<(MetaField, Filter)>> {
    let meta_field_filters = vec![
        Filter::Equal(FilterValue::UniqueIdentifier),
        Filter::Equal(FilterValue::UniqueIdentifier),
        Filter::Equal(FilterValue::UniqueIdentifier),
        Filter::Equal(FilterValue::UniqueIdentifier),
    ];
    let field_and_value = prop_oneof![
        select(meta_field_filters.clone()).prop_map(|filter| (MetaField::Owner, filter)),
        select(meta_field_filters.clone()).prop_map(|filter| (MetaField::DocumentId, filter)),
        select(meta_field_filters.clone()).prop_map(|filter| (MetaField::DocumentViewId, filter))
    ];
    vec(field_and_value, 1..3)
}

/// Top level strategy used for generating and injecting filter arguments derived from the fields
/// of a schema.
pub fn application_filters_strategy(
    schema_fields: Vec<SchemaField>,
) -> impl Strategy<Value = Vec<((FieldName, Filter), Vec<(FieldName, Filter)>)>> {
    let schema_fields = vec(select(schema_fields), 1..=3);
    let filters = schema_fields.prop_flat_map(|fields| {
        let mut filters = Vec::new();
        for field in fields {
            filters.push(application_field_filter_strategy(field))
        }
        filters
    });
    filters
}


/// Method for generating filter arguments for a single schema field. If the field is a relation
/// list type then sub-filters are also generated for the list collection.
fn application_field_filter_strategy(
    field: SchemaField,
) -> impl Strategy<Value = ((FieldName, Filter), Vec<(FieldName, Filter)>)> {
    match &field.field_type {
        SchemaFieldType::Boolean
        | SchemaFieldType::Integer
        | SchemaFieldType::Float
        | SchemaFieldType::String
        | SchemaFieldType::Relation
        | SchemaFieldType::PinnedRelation => generate_simple_field_filter(field.clone())
            .prop_map(|(name, filter)| ((name, filter), Vec::new()))
            .boxed(),
        SchemaFieldType::RelationList | SchemaFieldType::PinnedRelationList => {
            let name_and_filter = prop_oneof![
                (
                    Just(FilterValue::UniqueIdentifier),
                    Just(field.name.clone())
                )
                    .prop_map(|(value, name)| { (name, Filter::IsIn(value)) }),
                (
                    Just(FilterValue::UniqueIdentifier),
                    Just(field.name.clone())
                )
                    .prop_map(|(value, name)| { (name, Filter::NotIn(value)) }),
            ];
            let list_fields = field.relation_schema.clone().unwrap().fields;
            let list_filters: Vec<BoxedStrategy<(FieldName, Filter)>> = list_fields
                .iter()
                .map(|field| generate_simple_field_filter(field.clone()))
                .collect();
            (name_and_filter, list_filters).boxed()
        }
    }
}

/// Helper for generating a non-list type filter.
fn generate_simple_field_filter(field: SchemaField) -> BoxedStrategy<(FieldName, Filter)> {
    match &field.field_type {
        SchemaFieldType::Boolean => {
            let value_and_name_strategy = any::<bool>()
                .prop_map(FilterValue::Boolean)
                .prop_map(move |value| (field.name.clone(), value));

            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::Equal(value))),
                value_and_name_strategy.prop_map(|(name, value)| (name, Filter::NotEqual(value),))
            ]
            .boxed()
        }
        SchemaFieldType::Integer => {
            let value_and_name_strategy = any::<i64>()
                .prop_map(FilterValue::Integer)
                .prop_map(move |value| (field.name.clone(), value));
            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::Equal(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::IsIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::GreaterThanOrEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::LessThan(value))),
                value_and_name_strategy
                    .prop_map(|(name, value)| (name, Filter::LessThanOrEqual(value))),
            ]
            .boxed()
        }
        SchemaFieldType::Float => {
            let value_and_name_strategy = any::<f64>()
                .prop_map(FilterValue::Float)
                .prop_map(move |value| (field.name.clone(), value));
            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::Equal(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::IsIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::GreaterThanOrEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::LessThan(value))),
                value_and_name_strategy
                    .prop_map(|(name, value)| (name, Filter::LessThanOrEqual(value))),
            ]
            .boxed()
        }
        SchemaFieldType::String => {
            let value_and_name_strategy = any::<String>()
                .prop_map(FilterValue::String)
                .prop_map(move |value| (field.name.clone(), value));

            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::Contains(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotContains(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::Equal(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::IsIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::NotIn(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::GreaterThanOrEqual(value))),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(name, value)| (name, Filter::LessThan(value))),
                value_and_name_strategy
                    .prop_map(|(name, value)| (name, Filter::LessThanOrEqual(value))),
            ]
            .boxed()
        }
        SchemaFieldType::Relation | SchemaFieldType::PinnedRelation => prop_oneof![
            (
                Just(field.name.clone()),
                Just(FilterValue::UniqueIdentifier),
            )
                .prop_map(|(name, value)| (name, Filter::Equal(value))),
            (
                Just(field.name.clone()),
                Just(FilterValue::UniqueIdentifier),
            )
                .prop_map(|(name, value)| (name, Filter::NotEqual(value))),
            (
                Just(field.name.clone()),
                Just(FilterValue::UniqueIdentifier),
            )
                .prop_map(|(name, value)| (name, Filter::IsIn(value))),
            (
                Just(field.name.clone()),
                Just(FilterValue::UniqueIdentifier),
            )
                .prop_map(|(name, value)| (name, Filter::NotIn(value))),
        ]
        .boxed(),
        _ => panic!("Unexpected field type"),
    }
}
