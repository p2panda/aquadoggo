// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use async_recursion::async_recursion;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::test_utils::fixtures::random_key_pair;
use proptest_derive::Arbitrary;

use crate::proptests::document_strategies::{DocumentAST, FieldValue};
use crate::proptests::schema_strategies::{SchemaAST, SchemaFieldType};
use crate::test_utils::{add_document, add_schema, TestNode};

#[derive(Arbitrary, Debug, Clone)]
pub struct FieldName(#[proptest(regex = "[A-Za-z]{1}[A-Za-z0-9_]{0,63}")] String);

#[async_recursion]
pub async fn add_schemas_from_ast(node: &mut TestNode, schema: &SchemaAST) -> Schema {
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
                let schema = add_schemas_from_ast(node, &schema_ast).await;
                schema_fields.push((field.name, FieldType::Relation(schema.id().to_owned())));
            }
            SchemaFieldType::RelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast).await;
                schema_fields.push((field.name, FieldType::RelationList(schema.id().to_owned())));
            }
            SchemaFieldType::PinnedRelation => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast).await;
                schema_fields.push((
                    field.name,
                    FieldType::PinnedRelation(schema.id().to_owned()),
                ));
            }
            SchemaFieldType::PinnedRelationList => {
                let schema_ast = field.relation_schema.unwrap();
                let schema = add_schemas_from_ast(node, &schema_ast).await;
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

    node.context.schema_provider.update(schema.clone()).await;

    schema
}

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

    println!("Added document with view id: {} to the store", document_view_id);
    println!("Schema id: {}", &document_ast.schema_id);

    match documents.get_mut(&document_ast.schema_id) {
        Some(documents) => {
            println!("Insert document view into collection");
            documents.push(document_view_id.clone())
        }
        None => {
            println!("Create new document collection with initial view");
            documents.insert(
                document_ast.schema_id.clone(),
                vec![document_view_id.clone()],
            );
        }
    }

    document_view_id
}
