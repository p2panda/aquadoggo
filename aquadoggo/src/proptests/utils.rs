// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::entry::encode::encode_entry;
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::{EncodedEntry, EntryBuilder};
use p2panda_rs::operation::encode::encode_operation;
use p2panda_rs::operation::{EncodedOperation, OperationBuilder, OperationId, OperationValue};
use p2panda_rs::schema::{FieldType, Schema, SchemaId};
use p2panda_rs::test_utils::fixtures::random_key_pair;
use proptest_derive::Arbitrary;

use crate::proptests::document_strategies::{DocumentAST, FieldValue};
use crate::proptests::schema_strategies::{SchemaAST, SchemaFieldType};

#[derive(Arbitrary, Debug, Clone)]
pub struct FieldName(#[proptest(regex = "[A-Za-z]{1}[A-Za-z0-9_]{0,63}")] String);

pub fn convert_schema_ast(schema: &SchemaAST, schemas: &mut HashMap<SchemaId, Schema>) -> Schema {
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

pub fn encode_document_ast(
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
