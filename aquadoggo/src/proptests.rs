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

fn convert_schema_ast(schema: &SchemaAST, schemas: &mut HashMap<SchemaId, Schema>) -> Schema {
    let mut schema_fields: Vec<(FieldName, FieldType)> = vec![];

    for field in schema.fields.clone() {
        match field.field_type {
            SchemaFieldType::Boolean => {
                schema_fields.push((field.name, FieldType::Boolean));
            }
            SchemaFieldType::Integer => {
                schema_fields.push((field.name, FieldType::Integer));
            }
            SchemaFieldType::Float => {
                schema_fields.push((field.name, FieldType::Float));
            }
            SchemaFieldType::String => {
                schema_fields.push((field.name, FieldType::String));
            }
            SchemaFieldType::Relation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = convert_schema_ast(&schema_ast, schemas);
                schema_fields.push((field.name, FieldType::Relation(schema.id().to_owned())));
            }
            SchemaFieldType::RelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = convert_schema_ast(&schema_ast, schemas);
                schema_fields.push((field.name, FieldType::RelationList(schema.id().to_owned())));
            }
            SchemaFieldType::PinnedRelation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = convert_schema_ast(&schema_ast, schemas);
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelation(schema.id().to_owned()),
                ));
            }
            SchemaFieldType::PinnedRelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = convert_schema_ast(&schema_ast, schemas);
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelationList(schema.id().to_owned()),
                ));
            }
        }
    }

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
        // The proptest strategies for generating schema and deriving collections of documents for each injects
        // the raw AST types into the test. Here we convert these into p2panda `Entries`, `Operations` and `Schema`
        // which we can then use to populate a store and run queries against.

        let mut schemas = HashMap::new();
        let mut document_entries_and_operations = Vec::new();

        // Encode entries and operations for all generated schema, as well as converting into the p2panda `Schema` themselves.
        convert_schema_ast(&schema, &mut schemas);

        // For each derived document, encode entries and operations.
        for document in documents.iter() {
            encode_document_ast(document, &mut document_entries_and_operations);
        }

        // Some sanity checks
        assert!(schemas.len() > 0);
        assert!(documents.len() > 0);
        assert!(document_entries_and_operations.len() > 0);

        // Now we start up a test runner and inject a test node we can populate.
        test_runner(|node: TestNode| async move {
            // Add all schema to the schema provider.
            for (_, schema) in schemas.clone() {
                node.context.schema_provider.update(schema.clone()).await;
            };

            // Publish all document entries and operations to the node.
            for (entry, operation) in document_entries_and_operations {
                let plain_operation = decode_operation(&operation).unwrap();
                let schema = node.context.schema_provider.get(plain_operation.schema_id()).await.unwrap();
                let result = publish(&node.context.store, &schema, &entry,  &plain_operation, &operation).await;
                assert!(result.is_ok());
            }

            let query = |type_name: &SchemaId, args: &str| -> String {
                format!(
                    r#"{{
                    collection: all_{type_name}{args} {{
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
                }}"#
                )
            };

            for (_, schema) in schemas {
                // Configure and send test query.
                let client = graphql_test_client(&node).await;

                let response = client
                    .post("/graphql")
                    .json(&json!({"query": query(schema.id(), ""),}))
                    .send()
                    .await;

                let response: Response = response.json().await;
                assert!(response.is_ok());
            };
        });
    }
}
