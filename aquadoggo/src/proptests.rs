// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_graphql::Response;
use p2panda_rs::api::publish;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{EncodedEntry, EntryBuilder};
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::decode::decode_operation;
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::traits::Schematic;
use p2panda_rs::operation::{
    EncodedOperation, OperationBuilder, OperationFields, OperationId, OperationValue,
};
use p2panda_rs::schema::{FieldType, Schema, SchemaDescription, SchemaId, SchemaName};
use p2panda_rs::test_utils::fixtures::{random_document_view_id, random_key_pair};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde_json::json;

use crate::test_utils::{graphql_test_client, test_runner, TestNode};

const SCHEMA_NAME: &str = "test_schema";
const SCHEMA_DESCRIPTION: &str = "My test schema";

#[derive(Arbitrary, Debug, Clone)]
struct FieldName(#[proptest(regex = "[A-Za-z]{1}[A-Za-z0-9_]{0,63}")] String);

#[derive(Arbitrary, Debug, Clone)]
enum SchemaFieldType {
    Boolean,
    Integer,
    Float,
    String,
    Relation,
    RelationList,
    PinnedRelation,
    PinnedRelationList,
}

#[derive(Debug, Clone)]
struct SchemaField {
    name: FieldName,
    field_type: SchemaFieldType,
    relation_schema: Option<Box<SchemaAST>>,
}

#[derive(Debug, Clone)]
struct DocumentFieldValue {
    name: FieldName,
    value: FieldValue,
}

#[derive(Debug, Clone)]
struct DocumentAST {
    schema_id: SchemaId,
    fields: Vec<DocumentFieldValue>,
}

#[derive(Debug, Clone)]
struct SchemaAST {
    name: SchemaName,
    description: SchemaDescription,
    id: SchemaId,
    fields: Vec<SchemaField>,
}

impl SchemaAST {
    fn new(fields: Vec<SchemaField>) -> Self {
        let name = SchemaName::new("test_schema").unwrap();
        let description = SchemaDescription::new("My test schema").unwrap();
        let schema_id = SchemaId::Application(name.clone(), random_document_view_id());
        Self {
            name,
            description,
            id: schema_id,
            fields,
        }
    }
}

#[derive(Clone, Debug)]
enum FieldValue {
    /// Boolean value.
    Boolean(bool),

    /// Signed integer value.
    Integer(i64),

    /// Floating point value.
    Float(f64),

    /// String value.
    String(String),

    /// Reference to a document.
    Relation(DocumentAST),

    /// Reference to a list of documents.
    RelationList(Vec<DocumentAST>),

    /// Reference to a document view.
    PinnedRelation(DocumentAST),

    /// Reference to a list of document views.
    PinnedRelationList(Vec<DocumentAST>),
}

fn schema() -> impl Strategy<Value = SchemaAST> {
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
        2,  // We put up to 6 items per collection
        |inner| {
            prop_oneof![
                (any::<FieldName>(), vec(inner.clone(), 1..2)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::Relation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..2)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::RelationList,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..2)).prop_map(|(field_name, fields)| {
                    SchemaField {
                        name: field_name,
                        field_type: SchemaFieldType::PinnedRelation,
                        relation_schema: Some(Box::new(SchemaAST::new(fields))),
                    }
                }),
                (any::<FieldName>(), vec(inner.clone(), 1..2)).prop_map(|(field_name, fields)| {
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

fn documents_from_schema(schema: SchemaAST) -> impl Strategy<Value = Vec<DocumentAST>> {
    let schema_id = schema.id.clone();
    vec(values_from_schema(schema), 1..2).prop_map(move |documents| {
        documents
            .iter()
            .map(|document_fields| DocumentAST {
                fields: document_fields.to_owned(),
                schema_id: schema_id.clone(),
            })
            .collect::<Vec<DocumentAST>>()
    })
}

fn values_from_schema(schema: SchemaAST) -> impl Strategy<Value = Vec<DocumentFieldValue>> {
    let mut field_values = vec![];
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
            SchemaFieldType::RelationList => {
                vec(values_from_schema(*relation_schema.clone().unwrap()), 1..2)
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
                    .boxed()
            }
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
            SchemaFieldType::PinnedRelationList => {
                vec(values_from_schema(*relation_schema.clone().unwrap()), 1..2)
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
                    .boxed()
            }
        };
        field_values.push(value);
    }
    field_values
}

fn encode_schema_field(
    name: &FieldName,
    field_type: FieldType,
) -> (EncodedEntry, EncodedOperation) {
    let field_operation =
        encode_operation(&Schema::create_field(name.0.as_str(), field_type.clone()))
            .expect("Valid operations encode");
    let field_entry = EntryBuilder::new()
        .sign(&field_operation, &random_key_pair())
        .expect("Construct and sign entry");
    let field_entry = encode_entry(&field_entry).expect("Encode entry");
    (field_entry, field_operation)
}

fn encode_schema_ast(
    schema: &SchemaAST,
    entries_and_operations: &mut Vec<(EncodedEntry, EncodedOperation)>,
    schemas: &mut HashMap<SchemaId, Schema>,
) -> Schema {
    let mut schema_fields: Vec<(FieldName, FieldType)> = vec![];
    let mut field_hash_ids = vec![];

    for field in schema.fields.clone() {
        match field.field_type {
            SchemaFieldType::Boolean => {
                let entry_and_operation = encode_schema_field(&field.name, FieldType::Boolean);
                schema_fields.push((field.name, FieldType::Boolean));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::Integer => {
                let entry_and_operation = encode_schema_field(&field.name, FieldType::Integer);
                schema_fields.push((field.name, FieldType::Integer));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::Float => {
                let entry_and_operation = encode_schema_field(&field.name, FieldType::Float);
                schema_fields.push((field.name, FieldType::Float));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::String => {
                let entry_and_operation = encode_schema_field(&field.name, FieldType::String);
                schema_fields.push((field.name, FieldType::String));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::Relation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = encode_schema_ast(&schema_ast, entries_and_operations, schemas);
                let entry_and_operation =
                    encode_schema_field(&field.name, FieldType::Relation(schema.id().to_owned()));
                schema_fields.push((field.name, FieldType::Relation(schema.id().to_owned())));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::RelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = encode_schema_ast(&schema_ast, entries_and_operations, schemas);
                let entry_and_operation = encode_schema_field(
                    &field.name,
                    FieldType::RelationList(schema.id().to_owned()),
                );
                schema_fields.push((field.name, FieldType::RelationList(schema.id().to_owned())));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::PinnedRelation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = encode_schema_ast(&schema_ast, entries_and_operations, schemas);
                let entry_and_operation = encode_schema_field(
                    &field.name,
                    FieldType::PinnedRelation(schema.id().to_owned()),
                );
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelation(schema.id().to_owned()),
                ));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
            SchemaFieldType::PinnedRelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = encode_schema_ast(&schema_ast, entries_and_operations, schemas);
                let entry_and_operation = encode_schema_field(
                    &field.name,
                    FieldType::PinnedRelationList(schema.id().to_owned()),
                );
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelationList(schema.id().to_owned()),
                ));
                field_hash_ids.push(entry_and_operation.0.hash());
                entries_and_operations.push(entry_and_operation);
            }
        }
    }

    let schema_operation = encode_operation(&Schema::create(
        &schema.name.to_string(),
        &schema.description.to_string(),
        field_hash_ids
            .into_iter()
            .map(|hash| DocumentViewId::new(&vec![hash.into()]))
            .collect(),
    ))
    .expect("Valid operations encode");

    let schema_entry = EntryBuilder::new()
        .sign(&schema_operation, &random_key_pair())
        .expect("Construct and sign entry");
    let schema_entry = encode_entry(&schema_entry).expect("Encode entry");

    entries_and_operations.push((schema_entry, schema_operation));

    let schema = Schema::new(
        &schema.id,
        &schema.description.to_string(),
        &schema_fields
            .iter()
            .map(|(name, field_type)| (name.0.as_str(), field_type.to_owned()))
            .collect::<Vec<(&str, FieldType)>>(),
    )
    .expect("Generated schema is valid");

    schemas.insert(schema.id().clone(), schema.clone());

    schema
}

fn encode_document_ast(
    document_ast: &DocumentAST,
    entries_and_operations: &mut Vec<(EncodedEntry, EncodedOperation)>,
) -> OperationId {
    let mut operation_fields: Vec<(&str, OperationValue)> = vec![];

    for field in &document_ast.fields {
        match &field.value {
            FieldValue::Boolean(value) => {
                operation_fields.push((&field.name.0, value.to_owned().into()));
            }
            FieldValue::Integer(value) => {
                operation_fields.push((&field.name.0, value.to_owned().into()));
            }
            FieldValue::Float(value) => {
                operation_fields.push((&field.name.0, value.to_owned().into()));
            }
            FieldValue::String(value) => {
                operation_fields.push((&field.name.0, value.to_owned().into()));
            }
            FieldValue::Relation(value) => {
                let operation_id = encode_document_ast(&value, entries_and_operations);
                operation_fields.push((&field.name.0, DocumentId::new(&operation_id).into()));
            }
            FieldValue::RelationList(value) => {
                let mut document_ids = vec![];
                for fields in value {
                    let operation_id = encode_document_ast(&fields, entries_and_operations);
                    document_ids.push(DocumentId::new(&operation_id));
                }
                operation_fields.push((&field.name.0, document_ids.into()));
            }
            FieldValue::PinnedRelation(value) => {
                let operation_id = encode_document_ast(&value, entries_and_operations);
                operation_fields.push((&field.name.0, DocumentViewId::new(&[operation_id]).into()));
            }
            FieldValue::PinnedRelationList(value) => {
                let mut document_view_ids = vec![];
                for fields in value {
                    let operation_id = encode_document_ast(&fields, entries_and_operations);
                    document_view_ids.push(DocumentViewId::new(&[operation_id]));
                }
                operation_fields.push((&field.name.0, document_view_ids.into()));
            }
        }
    }

    let operation = OperationBuilder::new(&document_ast.schema_id)
        .fields(&operation_fields)
        .build()
        .expect("Build a valid document");

    let encoded_operation = encode_operation(&operation).unwrap();

    let entry = EntryBuilder::new()
        .sign(&encoded_operation, &random_key_pair())
        .expect("Construct and sign entry");
    let entry = encode_entry(&entry).expect("Encode entry");

    entries_and_operations.push((entry.clone(), encoded_operation));

    entry.hash().into()
}

prop_compose! {
    fn fields_and_values()(schema in schema())
                    (documents in documents_from_schema(schema.clone()), schema in Just(schema))
                    -> (SchemaAST, Vec<DocumentAST>) {
       (schema, documents)
   }
}

proptest! {
    #[test]
    fn test_add((schema, documents) in fields_and_values()) {
        let mut schema_entries_and_operations = Vec::new();
        let mut schemas = HashMap::new();
        let schema = encode_schema_ast(&schema, &mut schema_entries_and_operations, &mut schemas);
        schemas.insert(schema.id().clone(), schema);
        let mut document_entries_and_operations = Vec::new();
        for document in documents {
            encode_document_ast(&document, &mut document_entries_and_operations);
        }

        test_runner(|node: TestNode| async move {
            for (_, schema) in schemas.clone() {
                node.context.schema_provider.update(schema.clone()).await;
            };

            for (entry, operation) in schema_entries_and_operations {
                let plain_operation = decode_operation(&operation).unwrap();
                let schema = node.context.schema_provider.get(plain_operation.schema_id()).await.unwrap();
                let result = publish(&node.context.store, &schema, &entry,  &plain_operation, &operation).await;
                assert!(result.is_ok());
            }

            for (entry, operation) in document_entries_and_operations {
                let plain_operation = decode_operation(&operation).unwrap();
                let schema = node.context.schema_provider.get(plain_operation.schema_id()).await.unwrap();
                let result = publish(&node.context.store, &schema, &entry,  &plain_operation, &operation).await;
                assert!(result.is_ok());
            }

            for (_, schema) in schemas {
                // Configure and send test query.
                let client = graphql_test_client(&node).await;
                let query = format!(
                    r#"{{
                    collection: all_{type_name} {{
                        hasNextPage
                        totalCount
                        endCursor
                        documents {{
                            cursor
                            meta {{
                                owner
                                documentId
                                viewId
                            }}
                        }}
                    }},
                }}"#,
                    type_name = schema.id()
                );

                let response = client
                    .post("/graphql")
                    .json(&json!({
                        "query": query,
                    }))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());
            };
        });
    }
}
