// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use p2panda_rs::schema::{SchemaDescription, SchemaId, SchemaName};
use p2panda_rs::test_utils::fixtures::random_document_view_id;
use proptest::collection::vec;
use proptest::prelude::{any, Strategy};
use proptest::prop_oneof;
use proptest_derive::Arbitrary;

/// Name used of each generated schema.
const SCHEMA_NAME: &str = "test_schema";

/// Description used for each generated schema.
const SCHEMA_DESCRIPTION: &str = "My test schema";

/// Number of fields per schema. This value is shrunk during prop testing.
const FIELDS_PER_SCHEMA: usize = 10;

/// Desired number of nodes present in the schema AST.
const DESIRED_SCHEMA_NODES: u32 = 15;

/// Schema AST depth. This value is shrunk during prop testing.
///
/// Depth of `1` means a graph with root nodes and up to one layer of leaves.
const SCHEMA_DEPTH: u32 = 1;

/// Root of a schema AST.
#[derive(Debug, Clone, Eq, PartialEq)]
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

/// Field on a schema.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SchemaField {
    pub name: FieldName,
    pub field_type: SchemaFieldType,
    pub relation_schema: Option<Box<SchemaAST>>,
}

/// A fieldname which will follow the expected regex rules.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldName(#[proptest(regex = "[A-Za-z]{1}[A-Za-z0-9_]{0,63}")] pub String);

/// Types of field present on a schema.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq)]
pub enum SchemaFieldType {
    Boolean,
    Integer,
    Float,
    String,
    Bytes,
    Relation,
    RelationList,
    PinnedRelation,
    PinnedRelationList,
}

/// Strategy for generating a collection of schema.
pub fn schema_strategy() -> impl Strategy<Value = SchemaAST> {
    vec(schema_field(), 1..=FIELDS_PER_SCHEMA).prop_map(|schema_fields| {
        // Remove any fields with duplicate keys.
        let schema_fields_dedup_keys: HashMap<FieldName, SchemaField> = schema_fields
            .iter()
            .map(|field| (field.name.clone(), field.clone()))
            .collect();
        SchemaAST::new(schema_fields_dedup_keys.values().cloned().collect())
    })
}

/// Generate a collection of schema fields and recurse into relation fields.
fn schema_field() -> impl Strategy<Value = SchemaField> {
    // Selections for document AST leaves.
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
        any::<FieldName>().prop_map(|field_name| {
            SchemaField {
                name: field_name,
                field_type: SchemaFieldType::Bytes,
                relation_schema: None,
            }
        }),
    ];

    // Selections for the recursive fields.
    leaf.prop_recursive(
        SCHEMA_DEPTH,
        DESIRED_SCHEMA_NODES,
        FIELDS_PER_SCHEMA as u32,
        |inner| {
            prop_oneof![
                (
                    any::<FieldName>(),
                    vec(inner.clone(), 1..=FIELDS_PER_SCHEMA)
                )
                    .prop_map(|(field_name, fields)| {
                        SchemaField {
                            name: field_name,
                            field_type: SchemaFieldType::Relation,
                            relation_schema: Some(Box::new(SchemaAST::new(fields))),
                        }
                    }),
                (
                    any::<FieldName>(),
                    vec(inner.clone(), 1..=FIELDS_PER_SCHEMA)
                )
                    .prop_map(|(field_name, fields)| {
                        SchemaField {
                            name: field_name,
                            field_type: SchemaFieldType::RelationList,
                            relation_schema: Some(Box::new(SchemaAST::new(fields))),
                        }
                    }),
                (
                    any::<FieldName>(),
                    vec(inner.clone(), 1..=FIELDS_PER_SCHEMA)
                )
                    .prop_map(|(field_name, fields)| {
                        SchemaField {
                            name: field_name,
                            field_type: SchemaFieldType::PinnedRelation,
                            relation_schema: Some(Box::new(SchemaAST::new(fields))),
                        }
                    }),
                (
                    any::<FieldName>(),
                    vec(inner.clone(), 1..=FIELDS_PER_SCHEMA)
                )
                    .prop_map(|(field_name, fields)| {
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
