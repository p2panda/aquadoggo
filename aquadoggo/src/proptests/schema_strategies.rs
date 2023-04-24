// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::{SchemaDescription, SchemaId, SchemaName};
use p2panda_rs::test_utils::fixtures::random_document_view_id;
use proptest::collection::vec;
use proptest::prelude::{any, Strategy};
use proptest::prop_oneof;
use proptest_derive::Arbitrary;

use crate::proptests::utils::FieldName;

const SCHEMA_NAME: &str = "test_schema";
const SCHEMA_DESCRIPTION: &str = "My test schema";
const FIELDS_PER_SCHEMA: usize = 2;
const DESIRED_SCHEMA_NODES: u32 = 4;
const SCHEMA_DEPTH: u32 = 2;

#[derive(Debug, Clone)]
pub struct SchemaAST {
    pub name: SchemaName,
    pub description: SchemaDescription,
    pub id: SchemaId,
    pub fields: Vec<SchemaField>,
}

impl SchemaAST {
    fn new(fields: Vec<SchemaField>) -> Self {
        let name = SchemaName::new(SCHEMA_NAME).unwrap();
        let description = SchemaDescription::new(SCHEMA_DESCRIPTION).unwrap();
        let schema_id = SchemaId::Application(name.clone(), random_document_view_id());
        Self {
            name,
            description,
            id: schema_id,
            fields,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: FieldName,
    pub field_type: SchemaFieldType,
    pub relation_schema: Option<Box<SchemaAST>>,
}

#[derive(Arbitrary, Debug, Clone)]
pub enum SchemaFieldType {
    Boolean,
    Integer,
    Float,
    String,
    Relation,
    RelationList,
    PinnedRelation,
    PinnedRelationList,
}

pub fn schema_strategy() -> impl Strategy<Value = SchemaAST> {
    vec(schema_field(), 1..FIELDS_PER_SCHEMA).prop_map(|schema| SchemaAST::new(schema))
}

fn schema_field() -> impl Strategy<Value = SchemaField> {
    let leaf = prop_oneof![
        any::<FieldName>().prop_map(|field_name| {
            SchemaField {
                name: field_name,
                field_type: SchemaFieldType::Boolean,
                relation_schema: None,
            }
        }),
        any::<FieldName>().prop_map(|field_name| {
            SchemaField {
                name: field_name,
                field_type: SchemaFieldType::Integer,
                relation_schema: None,
            }
        }),
        any::<FieldName>().prop_map(|field_name| {
            SchemaField {
                name: field_name,
                field_type: SchemaFieldType::Float,
                relation_schema: None,
            }
        }),
        any::<FieldName>().prop_map(|field_name| {
            SchemaField {
                name: field_name,
                field_type: SchemaFieldType::String,
                relation_schema: None,
            }
        }),
    ];

    leaf.prop_recursive(
        SCHEMA_DEPTH,
        DESIRED_SCHEMA_NODES,
        FIELDS_PER_SCHEMA as u32,
        |inner| {
            prop_oneof![
                (any::<FieldName>(), vec(inner.clone(), 1..FIELDS_PER_SCHEMA)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::Relation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..FIELDS_PER_SCHEMA)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::RelationList,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..FIELDS_PER_SCHEMA)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::PinnedRelation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..FIELDS_PER_SCHEMA)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::PinnedRelationList,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                })
            ]
        },
    )
}
