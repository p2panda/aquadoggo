// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_recursion::async_recursion;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::test_utils::fixtures::{random_document_id, random_key_pair};
use proptest_derive::Arbitrary;

use crate::proptests::document_strategies::{DocumentAST, FieldValue};
use crate::proptests::schema_strategies::{SchemaAST, SchemaFieldType};
use crate::test_utils::{add_document, TestNode};

use super::filter_strategies::{Filter, FilterValue};

/// A fieldname which will follow the expected regex rules.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldName(#[proptest(regex = "[A-Za-z]{1}[A-Za-z0-9_]{0,63}")] pub String);

/// Add schemas from a schema AST to a test node.
#[async_recursion]
pub async fn add_schemas_from_ast(
    node: &mut TestNode,
    schema: &SchemaAST,
    schemas: &mut Vec<SchemaId>,
) -> Schema {
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
                let schema = add_schemas_from_ast(node, &schema_ast, schemas).await;
                schema_fields.push((field.name, FieldType::Relation(schema.id().to_owned())));
            }
            SchemaFieldType::RelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast, schemas).await;
                schema_fields.push((field.name, FieldType::RelationList(schema.id().to_owned())));
            }
            SchemaFieldType::PinnedRelation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast, schemas).await;
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelation(schema.id().to_owned()),
                ));
            }
            SchemaFieldType::PinnedRelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast, schemas).await;
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

    node.context
        .schema_provider
        .update(schema.clone())
        .await
        .expect("Can update schema");

    // We also populate a list of schema for easy checking.
    schemas.push(schema.id().to_owned());

    schema
}

/// Add documents from a document AST to the test node.
#[async_recursion]
pub async fn add_documents_from_ast(
    node: &mut TestNode,
    document_ast: &DocumentAST,
    documents: &mut HashMap<SchemaId, Vec<DocumentViewId>>,
) -> DocumentViewId {
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
            FieldValue::Relation(document_ast) => {
                let document_view_id = add_documents_from_ast(node, &document_ast, documents).await;
                let operation_id = document_view_id.graph_tips().first().unwrap();
                operation_fields.push((&field.name.0, DocumentId::new(&operation_id).into()));
            }
            FieldValue::RelationList(list) => {
                let mut document_ids = vec![];
                for document_ast in list {
                    let document_view_id =
                        add_documents_from_ast(node, &document_ast, documents).await;
                    let operation_id = document_view_id.graph_tips().first().unwrap();
                    document_ids.push(DocumentId::new(&operation_id));
                }
                operation_fields.push((&field.name.0, document_ids.into()));
            }
            FieldValue::PinnedRelation(document_ast) => {
                let document_view_id = add_documents_from_ast(node, &document_ast, documents).await;
                operation_fields.push((&field.name.0, document_view_id.into()));
            }
            FieldValue::PinnedRelationList(list) => {
                let mut document_view_ids = vec![];
                for document_ast in list {
                    let document_view_id =
                        add_documents_from_ast(node, &document_ast, documents).await;
                    document_view_ids.push(document_view_id);
                }
                operation_fields.push((&field.name.0, document_view_ids.into()));
            }
        }
    }

    let document_view_id = add_document(
        node,
        &document_ast.schema_id,
        operation_fields,
        &random_key_pair(),
    )
    .await;

    match documents.get_mut(&document_ast.schema_id) {
        Some(documents) => documents.push(document_view_id.clone()),
        None => {
            documents.insert(
                document_ast.schema_id.clone(),
                vec![document_view_id.clone()],
            );
        }
    }

    document_view_id
}

/// Helper for parsing the fields of a schema into valid GraphQL field selections. Any fields
/// which are a relation list type and have filter arguments passed will have the arguments added
/// to their sub-collection query.
pub async fn parse_selected_fields(
    node: &TestNode,
    schema: &Schema,
    list_filters: Option<&HashMap<&FieldName, Vec<String>>>,
) -> Vec<String> {
    let mut all_fields = Vec::new();
    for (name, field_type) in schema.fields().iter() {
        match field_type {
            FieldType::Relation(schema_id) | FieldType::PinnedRelation(schema_id) => {
                let schema = node
                    .context
                    .schema_provider
                    .get(&schema_id)
                    .await
                    .expect("Schema should exist on node");
                let fields = schema.fields().keys().join(" ");
                all_fields.push(format!("{name} {{ fields {{ {fields} }} }}"))
            }
            FieldType::RelationList(schema_id) | FieldType::PinnedRelationList(schema_id) => {
                let schema = node
                    .context
                    .schema_provider
                    .get(&schema_id)
                    .await
                    .expect("Schema should exist on node");
                let filter_args_str = match list_filters {
                    Some(list_filters) => {
                        if let Some(filters) = list_filters.get(&FieldName(name.to_owned())) {
                            let filter_args_str = filters.join(", ");
                            format!("( filter: {{ {filter_args_str} }} )")
                        } else {
                            "".to_string()
                        }
                    }
                    None => "".to_string(),
                };
                let fields = schema.fields().keys().join(" ");

                all_fields.push(format!(
                    "{name}{filter_args_str} {{ documents {{ fields {{ {fields} }} }} }}"
                ))
            }
            _ => all_fields.push(name.to_owned()),
        }
    }
    all_fields
}

/// Parse a single filter into a GraphQL argument string.
pub fn parse_filter(filter_args: &mut Vec<String>, name: &FieldName, filter: &Filter) {
    let name = name.clone().0;
    let document_id = random_document_id().to_string();
    match filter {
        Filter::Contains(value) => match value {
            FilterValue::String(value) => filter_args.push(format!(
                "{name}: {{ contains: {} }}",
                escape_string_value(value)
            )),
            _ => panic!(),
        },
        Filter::NotContains(value) => match value {
            FilterValue::String(value) => filter_args.push(format!(
                "{name}: {{ notContains: {} }}",
                escape_string_value(value)
            )),
            _ => panic!(),
        },
        Filter::Equal(value) => match value {
            FilterValue::UniqueIdentifier => {
                filter_args.push(format!("{name}: {{ eq: \"{document_id}\" }}"))
            }
            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
            FilterValue::String(value) => {
                filter_args.push(format!("{name}: {{ eq: {} }}", escape_string_value(value)))
            }
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ eq: {value} }}")),
        },
        Filter::NotEqual(value) => match value {
            FilterValue::UniqueIdentifier => {
                filter_args.push(format!("{name}: {{ notEq: \"{document_id}\" }}"))
            }
            FilterValue::Boolean(value) => {
                filter_args.push(format!("{name}: {{ notEq: {value} }}"))
            }
            FilterValue::String(value) => filter_args.push(format!(
                "{name}: {{ notEq: {} }}",
                escape_string_value(value)
            )),
            FilterValue::Integer(value) => {
                filter_args.push(format!("{name}: {{ notEq: {value} }}"))
            }
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ notEq: {value} }}")),
        },
        Filter::IsIn(value) => match value {
            FilterValue::UniqueIdentifier => {
                filter_args.push(format!("{name}: {{ in: [\"{document_id}\"] }}"))
            }
            FilterValue::Boolean(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
            FilterValue::String(value) => filter_args.push(format!(
                "{name}: {{ in: [{}] }}",
                escape_string_value(value)
            )),
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ in: [{value}] }}")),
        },
        Filter::NotIn(value) => match value {
            FilterValue::UniqueIdentifier => {
                filter_args.push(format!("{name}: {{ notIn: [\"{document_id}\"] }}"))
            }
            FilterValue::Boolean(value) => {
                filter_args.push(format!("{name}: {{ notIn: [{value}] }}"))
            }
            FilterValue::String(value) => filter_args.push(format!(
                "{name}: {{ notIn: [{}] }}",
                escape_string_value(value)
            )),
            FilterValue::Integer(value) => {
                filter_args.push(format!("{name}: {{ notIn: [{value}] }}"))
            }
            FilterValue::Float(value) => {
                filter_args.push(format!("{name}: {{ notIn: [{value}] }}"))
            }
        },
        Filter::GreaterThan(value) => match value {
            FilterValue::String(value) => {
                filter_args.push(format!("{name}: {{ gt: {} }}", escape_string_value(value)))
            }
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ gt: {value} }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ gt: {value} }}")),
            _ => panic!(),
        },
        Filter::LessThan(value) => match value {
            FilterValue::String(value) => {
                filter_args.push(format!("{name}: {{ lt: {} }}", escape_string_value(value)))
            }
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ lt: {value} }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ lt: {value} }}")),
            _ => panic!(),
        },
        Filter::GreaterThanOrEqual(value) => match value {
            FilterValue::String(value) => {
                filter_args.push(format!("{name}: {{ gte: {} }}", escape_string_value(value)))
            }
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ gte: {value} }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ gte: {value} }}")),
            _ => panic!(),
        },
        Filter::LessThanOrEqual(value) => match value {
            FilterValue::String(value) => {
                filter_args.push(format!("{name}: {{ lte: {} }}", escape_string_value(value)))
            }
            FilterValue::Integer(value) => filter_args.push(format!("{name}: {{ lte: {value} }}")),
            FilterValue::Float(value) => filter_args.push(format!("{name}: {{ lte: {value} }}")),
            _ => panic!(),
        },
    }
}

/// Escape unsafe chars in a string literal.
fn escape_string_value(value: &str) -> String {
    format!("\"{}\"", value.replace("\"", "").replace("\\", ""))
}
