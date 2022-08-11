// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::schema::FieldType;

/// Get the GraphQL type name for a p2panda field type.
///
/// GraphQL types for relations use the p2panda schema id as their name.
pub fn graphql_typename(operation_field_type: &FieldType) -> String {
    match operation_field_type {
        // Scalars
        FieldType::Boolean => "Boolean".to_string(),
        FieldType::Integer => "Int".to_string(),
        FieldType::Float => "Float".to_string(),
        FieldType::String => "String".to_string(),

        // Relations
        FieldType::Relation(schema_id) => schema_id.to_string(),
        FieldType::PinnedRelation(schema_id) => schema_id.to_string(),
        FieldType::RelationList(schema_id) => format!("[{}]", schema_id),
        FieldType::PinnedRelationList(schema_id) => format!("[{}]", schema_id),
    }
}

/// Make a simple [`MetaField`] with mostly default values.
pub fn metafield(name: &str, description: Option<&'static str>, type_name: &str) -> MetaField {
    MetaField {
        name: name.to_string(),
        description,
        ty: type_name.to_string(),
        args: Default::default(),
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

/// Make a simple object [`MetaType`] with mostly default values.
pub fn metaobject(
    name: &str,
    description: Option<&'static str>,
    fields: IndexMap<String, MetaField>,
) -> MetaType {
    MetaType::Object {
        name: name.to_string(),
        description,
        visible: Some(|_| true),
        fields,
        cache_control: Default::default(),
        extends: false,
        keys: None,
        is_subscription: false,
        // Dynamic query objects don't have an association to a Rust type.
        rust_typename: "__fake__",
    }
}
