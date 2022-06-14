// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::{FieldType, Schema};

pub fn get_schema_metafield(schema: &'static Schema) -> MetaField {
    MetaField {
        name: schema.id().as_str(),
        description: Some(schema.description()),
        args: Default::default(),
        ty: schema.id().as_str(),
        deprecation: Default::default(),
        cache_control: Default::default(),
        external: false,
        requires: None,
        provides: None,
        visible: None,
        compute_complexity: None,
        oneof: false,
    }
}

pub fn get_schema_metatype(schema: &'static Schema) -> MetaType {
    let mut fields = IndexMap::new();
    schema.fields().iter().for_each(|(field_name, field_type)| {
        fields.insert(
            field_name.to_string(),
            MetaField {
                name: field_name.to_string(),
                description: None,
                args: Default::default(),
                ty: get_graphql_type(field_type),
                deprecation: Default::default(),
                cache_control: Default::default(),
                external: false,
                requires: None,
                provides: None,
                visible: None,
                compute_complexity: None,
                oneof: false,
            },
        );
    });
    MetaType::Object {
        name: schema.id().as_str(),
        description: Some(schema.description()),
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        rust_typename: "__fake__",
    }
}

fn get_graphql_type(operation_field_type: &FieldType) -> String {
    match operation_field_type {
        FieldType::Bool => "Boolean".to_string(),
        FieldType::Int => "Int".to_string(),
        FieldType::Float => "Float".to_string(),
        FieldType::String => "String".to_string(),
        FieldType::Relation(schema) => schema.as_str(),
        FieldType::PinnedRelation(schema) => schema.as_str(),
        FieldType::RelationList(schema) => format!("[{}]", schema.as_str()),
        FieldType::PinnedRelationList(schema) => format!("[{}]", schema.as_str()),
    }
}
