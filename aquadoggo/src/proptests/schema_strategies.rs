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

#[derive(Debug, Clone)]
pub struct SchemaAST {
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
    vec(schema_field(), 1..2).prop_map(|schema| SchemaAST::new(schema))
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
        2,  // 2 levels deep
        25, // Shoot for maximum size of 25 nodes
        6,  // We put up to 6 items per collection
        |inner| {
            prop_oneof![
                (any::<FieldName>(), vec(inner.clone(), 1..6)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::Relation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..6)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::RelationList,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..6)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::PinnedRelation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..6)).prop_map(|(field_name, fields)| {
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
