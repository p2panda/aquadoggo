// SPDX-License-Identifier: AGPL-3.0-or-later

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::prop_oneof;
use proptest::sample::select;
use proptest::strategy::{Just, Strategy};
use proptest_derive::Arbitrary;

use crate::proptests::schema_strategies::SchemaFieldType;

use super::schema_strategies::SchemaField;
use super::utils::FieldName;

#[derive(Arbitrary, Debug, Clone)]
pub enum FilterValue {
    Boolean(bool),
    String(String),
    Integer(i64),
    Float(f64),
    UniqueIdentifier, // This is a placeholder for a document id, document view id or public key which is selected at testing time
}

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

#[derive(Arbitrary, Debug, Clone)]
pub enum MetaField {
    Owner,
    DocumentId,
    DocumentViewId,
}

pub fn application_filters_strategy(
    schema_fields: Vec<SchemaField>,
) -> impl Strategy<Value = Vec<(Filter, FieldName)>> {
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

pub fn application_field_filter_strategy(
    field: SchemaField,
) -> impl Strategy<Value = (Filter, FieldName)> {
    match &field.field_type {
        SchemaFieldType::Boolean => {
            let value_and_name_strategy = any::<bool>()
                .prop_map(FilterValue::Boolean)
                .prop_map(move |value| (value, field.name.clone()));

            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::Equal(value), name)),
                value_and_name_strategy.prop_map(|(value, name)| (Filter::NotEqual(value), name))
            ]
            .boxed()
        }
        SchemaFieldType::Integer => {
            let value_and_name_strategy = any::<i64>()
                .prop_map(FilterValue::Integer)
                .prop_map(move |value| (value, field.name.clone()));
            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::Equal(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::IsIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::GreaterThanOrEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::LessThan(value), name)),
                value_and_name_strategy
                    .prop_map(|(value, name)| (Filter::LessThanOrEqual(value), name)),
            ]
            .boxed()
        }
        SchemaFieldType::Float => {
            let value_and_name_strategy = any::<f64>()
                .prop_map(FilterValue::Float)
                .prop_map(move |value| (value, field.name.clone()));
            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::Equal(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::IsIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::GreaterThanOrEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::LessThan(value), name)),
                value_and_name_strategy
                    .prop_map(|(value, name)| (Filter::LessThanOrEqual(value), name)),
            ]
            .boxed()
        }
        SchemaFieldType::String => {
            let value_and_name_strategy = any::<String>()
                .prop_map(FilterValue::String)
                .prop_map(move |value| (value, field.name.clone()));

            prop_oneof![
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::Contains(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotContains(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::Equal(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::IsIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::NotIn(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::GreaterThanOrEqual(value), name)),
                value_and_name_strategy
                    .clone()
                    .prop_map(|(value, name)| (Filter::LessThan(value), name)),
                value_and_name_strategy
                    .prop_map(|(value, name)| (Filter::LessThanOrEqual(value), name)),
            ]
            .boxed()
        }
        SchemaFieldType::Relation | SchemaFieldType::PinnedRelation => prop_oneof![
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::Equal(value), name)),
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::NotEqual(value), name)),
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::IsIn(value), name)),
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::NotIn(value), name)),
        ]
        .boxed(),
        SchemaFieldType::RelationList | SchemaFieldType::PinnedRelationList => prop_oneof![
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::IsIn(value), name)),
            (
                Just(FilterValue::UniqueIdentifier),
                Just(field.name.clone())
            )
                .prop_map(|(value, name)| (Filter::NotIn(value), name)),
        ]
        .boxed(),
    }
}

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
