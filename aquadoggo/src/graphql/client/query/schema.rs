// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Write};

use async_graphql::indexmap::IndexMap;
use async_graphql::registry::{MetaField, MetaType};
use p2panda_rs::hash::Hash;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};

pub fn get_schema_metafield(schema: &Schema) -> MetaField {
    MetaField {
        name: schema.name().to_owned(),
        description: None,
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

pub fn get_schema_metatype(schema: &Schema) -> MetaType {
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
        description: None,
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

pub fn load_temp() -> Vec<Schema> {
    let file = File::open("./aquadoggo-schemas.temp").unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
}

pub fn save_temp(schemas: &Vec<Schema>) -> () {
    // let file = File::create("./aquadoggo-schemas.temp").unwrap();
    // serde_json::to_writer(file, &schemas).unwrap()
}

pub fn unlink_temp() -> () {
    // std::fs::remove_file("./aquadoggo-schemas.temp").unwrap()
}
