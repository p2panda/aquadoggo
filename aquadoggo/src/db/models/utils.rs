// SPDX-License-Identifier: AGPL-3.0-or-later

//! Utility methods for parsing database rows into p2panda data types.
use std::collections::BTreeMap;

use p2panda_rs::document::{DocumentId, DocumentViewFields, DocumentViewId, DocumentViewValue};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::traits::AsOperation;
use p2panda_rs::operation::{
    OperationAction, OperationBuilder, OperationId, OperationValue, PinnedRelation,
    PinnedRelationList, Relation, RelationList,
};
use p2panda_rs::schema::SchemaId;

use crate::db::models::DocumentViewFieldRow;
use crate::db::models::OperationFieldsJoinedRow;
use crate::db::types::StorageOperation;

/// Takes a vector of `OperationFieldsJoinedRow` and parses them into an `VerifiedOperation`
/// struct.
///
/// Operation fields which contain lists of values (RelationList & PinnedRelationList) are
/// flattened and inserted as indiviual rows. This means we need to reconstruct these fields when
/// retrieving an operation from the db.
pub fn parse_operation_rows(
    operation_rows: Vec<OperationFieldsJoinedRow>,
) -> Option<StorageOperation> {
    let first_row = match operation_rows.first() {
        Some(row) => row,
        None => return None,
    };

    // Unwrapping as we assume values coming from the db are valid
    let schema_id: SchemaId = first_row.schema_id.parse().unwrap();
    let public_key = PublicKey::new(&first_row.public_key).unwrap();
    let operation_id = first_row.operation_id.parse().unwrap();
    let document_id = first_row.document_id.parse().unwrap();
    let sorted_index = first_row.sorted_index;

    let mut relation_lists: BTreeMap<String, Vec<DocumentId>> = BTreeMap::new();
    let mut pinned_relation_lists: BTreeMap<String, Vec<DocumentViewId>> = BTreeMap::new();

    let mut operation_fields = Vec::new();

    // Iterate over returned field values, for each value:
    // * if it is a simple value type, parse it into an OperationValue and add it to the
    // operation_fields
    // * if it is a relation list value type: if the row.value is None then this list is empty and
    // we should create a relation list with no items, otherwise safely unwrap each item and parse
    // into a DocumentId/DocumentViewId then push to the suitable list vec
    if first_row.action != "delete" {
        operation_rows.iter().for_each(|row| {
            let field_type = row.field_type.as_ref().unwrap().as_str();
            let field_name = row.name.as_ref().unwrap();
            // We don't unwrap the value as for empty relation lists this may be `None`
            // Below we safely unwrap all values which are _not_ part of a relation list
            let field_value = row.value.as_ref();

            match field_type {
                "bool" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::Boolean(field_value.unwrap().parse::<bool>().unwrap()),
                    ));
                }
                "int" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::Integer(field_value.unwrap().parse::<i64>().unwrap()),
                    ));
                }
                "float" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::Float(field_value.unwrap().parse::<f64>().unwrap()),
                    ));
                }
                "str" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::String(field_value.unwrap().clone()),
                    ));
                }
                "bytes" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::Bytes(hex::decode(field_value.unwrap()).expect(
                            "bytes coming from the store are encoded in valid hex strings",
                        )),
                    ));
                }
                "relation" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::Relation(Relation::new(
                            field_value.unwrap().parse::<DocumentId>().unwrap(),
                        )),
                    ));
                }
                // This is a list item, so we push it to a vec but _don't_ add it
                // to the operation_fields yet.
                "relation_list" => {
                    match relation_lists.get_mut(field_name) {
                        // We unwrap the field value here as if the list already exists then we can
                        // assume this next item contains a value
                        Some(list) => {
                            list.push(field_value.unwrap().parse::<DocumentId>().unwrap())
                        }
                        None => {
                            let list = match field_value {
                                Some(document_id) => {
                                    vec![document_id.parse::<DocumentId>().unwrap()]
                                }
                                None => vec![],
                            };
                            relation_lists.insert(field_name.to_string(), list);
                        }
                    };
                }
                "pinned_relation" => {
                    operation_fields.push((
                        field_name.to_string(),
                        OperationValue::PinnedRelation(PinnedRelation::new(
                            field_value.unwrap().parse::<DocumentViewId>().unwrap(),
                        )),
                    ));
                }
                // This is a list item, so we push it to a vec but _don't_ add it
                // to the operation_fields yet.
                "pinned_relation_list" => {
                    match pinned_relation_lists.get_mut(field_name) {
                        // We unwrap the field value here as if the list already exists then we can
                        // assume this next item contains a value
                        Some(list) => {
                            list.push(field_value.unwrap().parse::<DocumentViewId>().unwrap())
                        }
                        None => {
                            let list = match field_value {
                                Some(document_view_id) => {
                                    vec![document_view_id.parse::<DocumentViewId>().unwrap()]
                                }
                                None => vec![],
                            };
                            pinned_relation_lists.insert(field_name.to_string(), list);
                        }
                    };
                }
                _ => (),
            };
        })
    };

    for (field_name, relation_list) in relation_lists {
        operation_fields.push((
            field_name,
            OperationValue::RelationList(RelationList::new(relation_list)),
        ));
    }

    for (field_name, pinned_relation_list) in pinned_relation_lists {
        operation_fields.push((
            field_name,
            OperationValue::PinnedRelationList(PinnedRelationList::new(pinned_relation_list)),
        ));
    }

    let operation_builder = OperationBuilder::new(&schema_id);
    let previous = first_row.previous.clone();
    let previous = previous.map(|previous| previous.parse().unwrap());
    let fields: Vec<(&str, OperationValue)> = operation_fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.to_owned()))
        .collect();

    let operation = match first_row.action.as_str() {
        "create" => operation_builder.fields(fields.as_slice()).build(),
        "update" => operation_builder
            .action(OperationAction::Update)
            .fields(fields.as_slice())
            .previous(previous.as_ref().unwrap())
            .build(),
        "delete" => operation_builder
            .action(OperationAction::Delete)
            .previous(previous.as_ref().unwrap())
            .build(),
        _ => panic!("Operation which was not CREATE, UPDATE or DELETE found."),
    }
    // Unwrap as we are sure values coming from the db are validated
    .unwrap();

    let operation = StorageOperation {
        document_id,
        id: operation_id,
        version: operation.version(),
        action: operation.action(),
        schema_id,
        previous: operation.previous(),
        fields: operation.fields(),
        public_key,
        sorted_index,
    };

    Some(operation)
}

/// Takes a single `OperationValue` and parses it into a vector of string values.
///
/// OperationValues are inserted into the database as strings. If a value is a list type
/// (`RelationList` & `PinnedRelationList`) we insert one row for each value. This method
/// transforms a single operation into a list of string values, if the is not a list, it will only
/// contain a single item.
pub fn parse_value_to_string_vec(value: &OperationValue) -> Vec<Option<String>> {
    match value {
        OperationValue::Boolean(bool) => vec![Some(bool.to_string())],
        OperationValue::Integer(int) => vec![Some(int.to_string())],
        OperationValue::Float(float) => vec![Some(float.to_string())],
        OperationValue::String(str) => vec![Some(str.to_string())],
        OperationValue::Relation(relation) => {
            vec![Some(relation.document_id().as_str().to_string())]
        }
        OperationValue::RelationList(relation_list) => {
            let mut db_values = Vec::new();
            if relation_list.len() == 0 {
                db_values.push(None);
            } else {
                for document_id in relation_list.iter() {
                    db_values.push(Some(document_id.to_string()))
                }
            }
            db_values
        }
        OperationValue::PinnedRelation(pinned_relation) => {
            vec![Some(pinned_relation.view_id().to_string())]
        }
        OperationValue::PinnedRelationList(pinned_relation_list) => {
            let mut db_values = Vec::new();
            if pinned_relation_list.len() == 0 {
                db_values.push(None);
            } else {
                for document_view_id in pinned_relation_list.iter() {
                    db_values.push(Some(document_view_id.to_string()))
                }
            }
            db_values
        }
        OperationValue::Bytes(bytes) => {
            // bytes are stored in the db as hex strings
            vec![Some(hex::encode(bytes))]
        }
    }
}

/// Takes a vector of `DocumentViewFieldRow` and parses them into an `DocumentViewFields` struct.
///
/// Document fields which contain lists of values (RelationList & PinnedRelationList) are flattened
/// and inserted as indiviual rows. This means we need to reconstruct these fields when retrieving
/// an document view from the db.
pub fn parse_document_view_field_rows(
    document_field_rows: Vec<DocumentViewFieldRow>,
) -> DocumentViewFields {
    let mut relation_lists: BTreeMap<String, (OperationId, Vec<DocumentId>)> = BTreeMap::new();
    let mut pinned_relation_lists: BTreeMap<String, (OperationId, Vec<DocumentViewId>)> =
        BTreeMap::new();

    let mut document_view_fields = DocumentViewFields::new();

    // Iterate over returned field values, for each value:
    // - if it is a simple value type, safely unwrap it, parse it into an DocumentViewValue and add
    // it to the document_view_fields
    // - if it is a relation list value type: if the row.value is None then this list is empty and
    // we should create a relation list with no items, otherwise safely unwrap each item and parse
    // into a DocumentId/DocumentViewId then push to the suitable list vec
    document_field_rows.iter().for_each(|row| {
        match row.field_type.as_str() {
            "bool" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::Boolean(
                            row.value.as_ref().unwrap().parse::<bool>().unwrap(),
                        ),
                    ),
                );
            }
            "int" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::Integer(
                            row.value.as_ref().unwrap().parse::<i64>().unwrap(),
                        ),
                    ),
                );
            }
            "float" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::Float(row.value.as_ref().unwrap().parse::<f64>().unwrap()),
                    ),
                );
            }
            "str" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::String(row.value.as_ref().unwrap().clone()),
                    ),
                );
            }
            "bytes" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::Bytes(
                            hex::decode(row.value.as_ref().unwrap())
                                .expect("bytes coming from the db to be hex encoded"),
                        ),
                    ),
                );
            }
            "relation" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::Relation(Relation::new(
                            row.value.as_ref().unwrap().parse::<DocumentId>().unwrap(),
                        )),
                    ),
                );
            }
            // This is a list item, so we push it to a vec but _don't_ add it
            // to the document_view_fields yet.
            "relation_list" => {
                match relation_lists.get_mut(&row.name) {
                    Some((_, list)) => {
                        list.push(row.value.as_ref().unwrap().parse::<DocumentId>().unwrap())
                    }
                    None => {
                        let list = match row.value.as_ref() {
                            Some(document_id) => {
                                vec![document_id.parse::<DocumentId>().unwrap()]
                            }
                            None => vec![],
                        };
                        relation_lists
                            .insert(row.name.clone(), (row.operation_id.parse().unwrap(), list));
                    }
                };
            }
            "pinned_relation" => {
                document_view_fields.insert(
                    &row.name,
                    DocumentViewValue::new(
                        &row.operation_id.parse::<OperationId>().unwrap(),
                        &OperationValue::PinnedRelation(PinnedRelation::new(
                            row.value
                                .as_ref()
                                .unwrap()
                                .parse::<DocumentViewId>()
                                .unwrap(),
                        )),
                    ),
                );
            }
            // This is a list item, so we push it to a vec but _don't_ add it to the
            // document_view_fields yet.
            "pinned_relation_list" => {
                match pinned_relation_lists.get_mut(&row.name) {
                    Some((_, list)) => list.push(
                        row.value
                            .as_ref()
                            .unwrap()
                            .parse::<DocumentViewId>()
                            .unwrap(),
                    ),
                    None => {
                        let list = match row.value.as_ref() {
                            Some(document_view_id) => {
                                vec![document_view_id.parse::<DocumentViewId>().unwrap()]
                            }
                            None => vec![],
                        };
                        pinned_relation_lists
                            .insert(row.name.clone(), (row.operation_id.parse().unwrap(), list));
                    }
                };
            }
            _ => (),
        };
    });

    for (field_name, (operation_id, relation_list)) in relation_lists {
        document_view_fields.insert(
            &field_name,
            DocumentViewValue::new(
                &operation_id,
                &OperationValue::RelationList(RelationList::new(relation_list)),
            ),
        );
    }

    for (field_name, (operation_id, pinned_relation_list)) in pinned_relation_lists {
        document_view_fields.insert(
            &field_name,
            DocumentViewValue::new(
                &operation_id,
                &OperationValue::PinnedRelationList(PinnedRelationList::new(pinned_relation_list)),
            ),
        );
    }

    document_view_fields
}

#[cfg(test)]
mod tests {
    use std::vec;

    use p2panda_rs::document::DocumentViewValue;
    use p2panda_rs::operation::traits::AsOperation;
    use p2panda_rs::operation::{
        OperationId, OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
    };
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::test_utils::fixtures::{create_operation, schema_id};
    use rstest::rstest;

    use crate::db::models::{DocumentViewFieldRow, OperationFieldsJoinedRow};
    use crate::test_utils::doggo_fields;

    use super::{parse_document_view_field_rows, parse_operation_rows, parse_value_to_string_vec};

    #[test]
    fn parses_operation_rows() {
        let operation_rows = vec![
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("age".to_string()),
                field_type: Some("int".to_string()),
                value: Some("28".to_string()),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("data".to_string()),
                field_type: Some("bytes".to_string()),
                value: Some("00010203".to_string()),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("height".to_string()),
                field_type: Some("float".to_string()),
                value: Some("3.5".to_string()),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("is_admin".to_string()),
                field_type: Some("bool".to_string()),
                value: Some("false".to_string()),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_profile_pictures".to_string()),
                field_type: Some("relation_list".to_string()),
                value: Some(
                    "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                ),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_profile_pictures".to_string()),
                field_type: Some("relation_list".to_string()),
                value: Some(
                    "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                ),
                list_index: Some(1),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_special_profile_pictures".to_string()),
                field_type: Some("pinned_relation_list".to_string()),
                value: Some(
                    "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_string(),
                ),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_special_profile_pictures".to_string()),
                field_type: Some("pinned_relation_list".to_string()),
                value: Some(
                    "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_string(),
                ),
                list_index: Some(1),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_special_dog_pictures".to_string()),
                field_type: Some("pinned_relation_list".to_string()),
                value: Some(
                    "0020bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc"
                        .to_string(),
                ),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("many_special_dog_pictures".to_string()),
                field_type: Some("pinned_relation_list".to_string()),
                value: Some(
                    "0020abababababababababababababababababababababababababababababababab"
                        .to_string(),
                ),
                list_index: Some(1),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("profile_picture".to_string()),
                field_type: Some("relation".to_string()),
                value: Some(
                    "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        .to_string(),
                ),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("special_profile_picture".to_string()),
                field_type: Some("pinned_relation".to_string()),
                value: Some(
                    "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        .to_string(),
                ),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("username".to_string()),
                field_type: Some("str".to_string()),
                value: Some("bubu".to_string()),
                list_index: Some(0),
                sorted_index: None,
            },
            OperationFieldsJoinedRow {
                public_key: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous: None,
                name: Some("an_empty_relation_list".to_string()),
                field_type: Some("pinned_relation_list".to_string()),
                value: None,
                list_index: Some(0),
                sorted_index: None,
            },
        ];

        let operation = parse_operation_rows(operation_rows).unwrap();

        assert_eq!(
            operation.fields().unwrap().get("username").unwrap(),
            &OperationValue::String("bubu".to_string())
        );
        assert_eq!(
            operation.fields().unwrap().get("data").unwrap(),
            &OperationValue::Bytes(vec![0, 1, 2, 3])
        );
        assert_eq!(
            operation.fields().unwrap().get("age").unwrap(),
            &OperationValue::Integer(28)
        );
        assert_eq!(
            operation.fields().unwrap().get("height").unwrap(),
            &OperationValue::Float(3.5)
        );
        assert_eq!(
            operation.fields().unwrap().get("is_admin").unwrap(),
            &OperationValue::Boolean(false)
        );
        assert_eq!(
            operation
                .fields()
                .unwrap()
                .get("many_profile_pictures")
                .unwrap(),
            &OperationValue::RelationList(RelationList::new(vec![
                "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .unwrap(),
                "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .unwrap(),
            ]))
        );
        assert_eq!(
            operation
                .fields()
                .unwrap()
                .get("many_special_profile_pictures")
                .unwrap(),
            &OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .parse()
                    .unwrap(),
                "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .parse()
                    .unwrap(),
            ]))
        );
        assert_eq!(
            operation
                .fields()
                .unwrap()
                .get("many_special_dog_pictures")
                .unwrap(),
            &OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                "0020bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc"
                    .parse()
                    .unwrap(),
                "0020abababababababababababababababababababababababababababababababab"
                    .parse()
                    .unwrap(),
            ]))
        );

        assert_eq!(
            operation.fields().unwrap().get("profile_picture").unwrap(),
            &OperationValue::Relation(Relation::new(
                "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    .parse()
                    .unwrap()
            ))
        );
        assert_eq!(
            operation
                .fields()
                .unwrap()
                .get("special_profile_picture")
                .unwrap(),
            &OperationValue::PinnedRelation(PinnedRelation::new(
                "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .parse()
                    .unwrap()
            ))
        );
        assert_eq!(
            operation
                .fields()
                .unwrap()
                .get("an_empty_relation_list")
                .unwrap(),
            &OperationValue::PinnedRelationList(PinnedRelationList::new(vec![]))
        )
    }

    #[rstest]
    fn operation_values_to_string_vec(schema_id: SchemaId) {
        let expected_list = vec![
            Some("28".into()),
            None, // This is an empty relation list
            Some("0020abababababababababababababababababababababababababababababababab".into()),
            Some("0020cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd".into()),
            Some("00010203".into()),
            Some("3.5".into()),
            Some("false".into()),
            Some("0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
            Some("0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
            Some("0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into()),
            Some("0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into()),
            Some("0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".into()),
            Some("0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into()),
            Some("bubu".into()),
        ];

        let operation = create_operation(doggo_fields(), schema_id);

        let mut string_value_list = vec![];
        for (_, value) in operation.fields().unwrap().iter() {
            string_value_list.push(parse_value_to_string_vec(value));
        }

        let string_value_list: Vec<Option<String>> =
            string_value_list.into_iter().flatten().collect();
        assert_eq!(expected_list, string_value_list)
    }

    #[rstest]
    fn parses_empty_relation_lists_correctly(schema_id: SchemaId) {
        let expected_list = vec![None];

        let operation = create_operation(
            vec![(
                "field_name",
                OperationValue::RelationList(RelationList::new(vec![])),
            )],
            schema_id,
        );

        let mut string_value_list = vec![];
        for (_, value) in operation.fields().unwrap().iter() {
            string_value_list.push(parse_value_to_string_vec(value));
        }

        let string_value_list: Vec<Option<String>> =
            string_value_list.into_iter().flatten().collect();
        assert_eq!(expected_list, string_value_list)
    }

    #[test]
    fn parses_document_field_rows() {
        let document_id =
            "0020713b2777f1222660291cb528d220c358920b4beddc1aea9df88a69cec45a10c0".to_string();
        let operation_id =
            "0020dc8fe1cbacac4d411ae25ea264369a7b2dabdfb617129dec03b6661edd963770".to_string();
        let document_view_id = operation_id.clone();

        let document_field_rows = vec![
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "age".to_string(),
                list_index: 0,
                field_type: "int".to_string(),
                value: Some("28".to_string()),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "height".to_string(),
                list_index: 0,
                field_type: "float".to_string(),
                value: Some("3.5".to_string()),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "is_admin".to_string(),
                list_index: 0,
                field_type: "bool".to_string(),
                value: Some("false".to_string()),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "many_profile_pictures".to_string(),
                list_index: 0,
                field_type: "relation_list".to_string(),
                value: Some(
                    "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "many_profile_pictures".to_string(),
                list_index: 1,
                field_type: "relation_list".to_string(),
                value: Some(
                    "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "many_special_profile_pictures".to_string(),
                list_index: 0,
                field_type: "pinned_relation_list".to_string(),
                value: Some(
                    "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "many_special_profile_pictures".to_string(),
                list_index: 1,
                field_type: "pinned_relation_list".to_string(),
                value: Some(
                    "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "profile_picture".to_string(),
                list_index: 0,
                field_type: "relation".to_string(),
                value: Some(
                    "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "special_profile_picture".to_string(),
                list_index: 0,
                field_type: "pinned_relation".to_string(),
                value: Some(
                    "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        .to_string(),
                ),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "username".to_string(),
                list_index: 0,
                field_type: "str".to_string(),
                value: Some("bubu".to_string()),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "data".to_string(),
                list_index: 0,
                field_type: "bytes".to_string(),
                value: Some("00010203".to_string()),
            },
            DocumentViewFieldRow {
                document_id: document_id.clone(),
                document_view_id: document_view_id.clone(),
                operation_id: operation_id.clone(),
                name: "an_empty_relation_list".to_string(),
                list_index: 0,
                field_type: "pinned_relation_list".to_string(),
                value: None,
            },
        ];

        let document_fields = parse_document_view_field_rows(document_field_rows);
        let operation_id: OperationId =
            "0020dc8fe1cbacac4d411ae25ea264369a7b2dabdfb617129dec03b6661edd963770"
                .parse()
                .unwrap();

        assert_eq!(
            document_fields.get("username").unwrap(),
            &DocumentViewValue::new(&operation_id, &OperationValue::String("bubu".to_string()))
        );
        assert_eq!(
            document_fields.get("data").unwrap(),
            &DocumentViewValue::new(&operation_id, &OperationValue::Bytes(vec![0, 1, 2, 3]))
        );
        assert_eq!(
            document_fields.get("age").unwrap(),
            &DocumentViewValue::new(&operation_id, &OperationValue::Integer(28))
        );
        assert_eq!(
            document_fields.get("height").unwrap(),
            &DocumentViewValue::new(&operation_id, &OperationValue::Float(3.5))
        );
        assert_eq!(
            document_fields.get("is_admin").unwrap(),
            &DocumentViewValue::new(&operation_id, &OperationValue::Boolean(false))
        );
        assert_eq!(
            document_fields.get("many_profile_pictures").unwrap(),
            &DocumentViewValue::new(
                &operation_id,
                &OperationValue::RelationList(RelationList::new(vec![
                    "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .parse()
                        .unwrap(),
                    "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .parse()
                        .unwrap(),
                ]))
            )
        );
        assert_eq!(
            document_fields
                .get("many_special_profile_pictures")
                .unwrap(),
            &DocumentViewValue::new(
                &operation_id,
                &OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                    "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .parse()
                        .unwrap(),
                    "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .parse()
                        .unwrap(),
                ]))
            )
        );
        assert_eq!(
            document_fields.get("profile_picture").unwrap(),
            &DocumentViewValue::new(
                &operation_id,
                &OperationValue::Relation(Relation::new(
                    "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        .parse()
                        .unwrap()
                ))
            )
        );
        assert_eq!(
            document_fields.get("special_profile_picture").unwrap(),
            &DocumentViewValue::new(
                &operation_id,
                &OperationValue::PinnedRelation(PinnedRelation::new(
                    "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        .parse()
                        .unwrap()
                ))
            )
        );
        assert_eq!(
            document_fields.get("an_empty_relation_list").unwrap(),
            &DocumentViewValue::new(
                &operation_id,
                &OperationValue::PinnedRelationList(PinnedRelationList::new(vec![]))
            )
        )
    }
}
