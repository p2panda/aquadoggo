// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::SchemaId;
use proptest::collection::vec;
use proptest::prelude::{any, Strategy};
use proptest_derive::Arbitrary;

use crate::proptests::strategies::schema::{FieldName, SchemaAST, SchemaFieldType};

/// The maximum number of documents which will be generated per schema.
/// This is one value which will be shrunk if a failure occurs.
const MAX_DOCUMENTS_PER_ROOT_SCHEMA: usize = 15;

/// The maximum number of document relations which will be generated in a relation list.
/// Same as above, this is a shrinking value.
const MAX_DOCUMENTS_PER_RELATION_LIST: usize = 2;

/// A hexadecimal string.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, Hash)]
pub struct HexString(#[proptest(regex = "([a-fA-F0-9]{2}){0,64}")] pub String);

/// AST representing a document and its relations.
#[derive(Debug, Clone)]
pub struct DocumentAST {
    pub schema_id: SchemaId,
    pub fields: Vec<DocumentFieldValue>,
}

/// A single field name and value from a document. Is used when building nested document representations.
#[derive(Debug, Clone)]
pub struct DocumentFieldValue {
    pub name: FieldName,
    pub value: FieldValue,
}

/// A Value on a document field.
#[derive(Clone, Debug)]
pub enum FieldValue {
    /// Boolean value.
    Boolean(bool),

    /// Signed integer value.
    Integer(i64),

    /// Floating point value.
    Float(f64),

    /// String value.
    String(String),

    /// Hex encoded bytes value.
    Bytes(Vec<u8>),

    /// Reference to a document.
    Relation(DocumentAST),

    /// Reference to a list of documents.
    RelationList(Vec<DocumentAST>),

    /// Reference to a document view.
    PinnedRelation(DocumentAST),

    /// Reference to a list of document views.
    PinnedRelationList(Vec<DocumentAST>),
}

/// Strategy for generating a collection af document ASTs from a single schema AST.
pub fn documents_strategy(schema: SchemaAST) -> impl Strategy<Value = Vec<DocumentAST>> {
    let schema_id = schema.id.clone();
    vec(values_from_schema(schema), 0..MAX_DOCUMENTS_PER_ROOT_SCHEMA).prop_map(move |documents| {
        documents
            .iter()
            .map(|document_fields| DocumentAST {
                fields: document_fields.to_owned(),
                schema_id: schema_id.clone(),
            })
            .collect::<Vec<DocumentAST>>()
    })
}

/// Strategy for generating a collection of document field values corresponding to fields keys and
/// types defined on a schema AST.
fn values_from_schema(schema: SchemaAST) -> impl Strategy<Value = Vec<DocumentFieldValue>> {
    let mut field_values = vec![];

    // For every field in the document generate a field value. Recurses into relation fields.
    for schema_field in schema.fields {
        let field_name = schema_field.name.clone();
        let relation_schema = schema_field.relation_schema.clone();
        let value = match schema_field.field_type {
            SchemaFieldType::Boolean => any::<bool>()
                .prop_map(move |value| {
                    let value = FieldValue::Boolean(value);
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value,
                    }
                })
                .boxed(),
            SchemaFieldType::Integer => any::<i64>()
                .prop_map(move |value| {
                    let value = FieldValue::Integer(value);
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value,
                    }
                })
                .boxed(),
            SchemaFieldType::Float => any::<f64>()
                .prop_map(move |value| {
                    let value = FieldValue::Float(value);
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value,
                    }
                })
                .boxed(),
            SchemaFieldType::String => any::<String>()
                .prop_map(move |value| {
                    let value = FieldValue::String(value);
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value,
                    }
                })
                .boxed(),
            SchemaFieldType::Bytes => any::<Vec<u8>>()
                .prop_map(move |value| {
                    let value = FieldValue::Bytes(value);
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value,
                    }
                })
                .boxed(),
            SchemaFieldType::Relation => values_from_schema(*relation_schema.clone().unwrap())
                .prop_map(move |value| {
                    let schema_id = relation_schema.clone().unwrap().id.clone();
                    let document_ast = DocumentAST {
                        schema_id: schema_id.clone(),
                        fields: value.clone(),
                    };
                    DocumentFieldValue {
                        name: field_name.clone(),
                        value: FieldValue::Relation(document_ast),
                    }
                })
                .boxed(),
            SchemaFieldType::RelationList => vec(
                values_from_schema(*relation_schema.clone().unwrap()),
                0..MAX_DOCUMENTS_PER_RELATION_LIST,
            )
            .prop_map(move |value| {
                let schema_id = relation_schema.clone().unwrap().id.clone();
                let document_asts = value
                    .into_iter()
                    .map(|document_fields| DocumentAST {
                        schema_id: schema_id.clone(),
                        fields: document_fields,
                    })
                    .collect();
                DocumentFieldValue {
                    name: field_name.clone(),
                    value: FieldValue::RelationList(document_asts),
                }
            })
            .boxed(),
            SchemaFieldType::PinnedRelation => {
                values_from_schema(*relation_schema.clone().unwrap())
                    .prop_map(move |value| {
                        let schema_id = relation_schema.clone().unwrap().id.clone();
                        let document_ast = DocumentAST {
                            schema_id: schema_id.clone(),
                            fields: value.clone(),
                        };
                        DocumentFieldValue {
                            name: field_name.clone(),
                            value: FieldValue::PinnedRelation(document_ast),
                        }
                    })
                    .boxed()
            }
            SchemaFieldType::PinnedRelationList => vec(
                values_from_schema(*relation_schema.clone().unwrap()),
                0..MAX_DOCUMENTS_PER_RELATION_LIST,
            )
            .prop_map(move |value| {
                let schema_id = relation_schema.clone().unwrap().id.clone();
                let document_asts = value
                    .into_iter()
                    .map(|document_fields| DocumentAST {
                        schema_id: schema_id.clone(),
                        fields: document_fields,
                    })
                    .collect();
                DocumentFieldValue {
                    name: field_name.clone(),
                    value: FieldValue::PinnedRelationList(document_asts),
                }
            })
            .boxed(),
        };
        field_values.push(value);
    }
    field_values
}
