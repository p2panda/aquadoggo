// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::{
    Operation, OperationFields, OperationId, OperationValue, PinnedRelation, PinnedRelationList,
    Relation, RelationList,
};
use p2panda_rs::schema::SchemaId;

use crate::db::models::operation::OperationFieldsJoinedRow;
use crate::db::stores::operation::OperationStorage;

/// Takes a vector of `OperationFieldsJoinedRow` and parses them into an `OperationStorage`
/// struct.
///
/// Operation fields which contain lists of values (RelationList & PinnedRelationList) are
/// flattened and inserted as indiviual rows. This means we need to reconstruct these fields
/// when retrieving an operation from the db.
pub fn parse_operation_rows(
    operation_rows: Vec<OperationFieldsJoinedRow>,
) -> Option<OperationStorage> {
    let first_row = match operation_rows.get(0) {
        Some(row) => row,
        None => return None,
    };

    // Unwrapping as we assume values coming from the db are valid
    let schema: SchemaId = first_row.schema_id.parse().unwrap();
    let author = Author::new(&first_row.author).unwrap();
    let operation_id = first_row.operation_id.parse().unwrap();
    let document_id = first_row.document_id.parse().unwrap();

    // TODO: Once we have resolved https://github.com/p2panda/p2panda/issues/315 then
    // we can coerce types here.
    let mut previous_operations: Vec<OperationId> = Vec::new();
    if first_row.action != "create" {
        previous_operations = first_row
            .previous_operations
            .rsplit('_')
            .map(|id_str| Hash::new(id_str).unwrap().into())
            .collect();
    }

    let mut relation_list: Vec<DocumentId> = Vec::new();
    let mut pinned_relation_list: Vec<DocumentViewId> = Vec::new();

    let mut operation_fields = OperationFields::new();

    // Iterate over returned field values, for each value:
    //  - if it is a simple value type, parse it into an OperationValue and add it to the operation_fields
    //  - if it is a relation list value type parse each item into a DocumentId/DocumentViewId and push to
    //    the suitable vec (instantiated above)
    operation_rows.iter().for_each(|row| {
        match row.field_type.as_str() {
            "bool" => {
                operation_fields
                    .add(
                        row.name.as_str(),
                        OperationValue::Boolean(row.value.parse::<bool>().unwrap()),
                    )
                    .unwrap();
            }
            "int" => {
                operation_fields
                    .add(
                        row.name.as_str(),
                        OperationValue::Integer(row.value.parse::<i64>().unwrap()),
                    )
                    .unwrap();
            }
            "float" => {
                operation_fields
                    .add(
                        row.name.as_str(),
                        OperationValue::Float(row.value.parse::<f64>().unwrap()),
                    )
                    .unwrap();
            }
            "str" => {
                operation_fields
                    .add(row.name.as_str(), OperationValue::Text(row.value.clone()))
                    .unwrap();
            }
            "relation" => {
                operation_fields
                    .add(
                        row.name.as_str(),
                        OperationValue::Relation(Relation::new(
                            row.value.parse::<DocumentId>().unwrap(),
                        )),
                    )
                    .unwrap();
            }
            // This is a list item, so we push it to a vec but _don't_ add it
            // to the operation_fields yet.
            "relation_list" => relation_list.push(row.value.parse::<DocumentId>().unwrap()),
            "pinned_relation" => {
                operation_fields
                    .add(
                        row.name.as_str(),
                        OperationValue::PinnedRelation(PinnedRelation::new(
                            row.value.parse::<DocumentViewId>().unwrap(),
                        )),
                    )
                    .unwrap();
            }
            // This is a list item, so we push it to a vec but _don't_ add it
            // to the operation_fields yet.
            "pinned_relation_list" => {
                pinned_relation_list.push(row.value.parse::<DocumentViewId>().unwrap())
            }
            _ => (),
        };
    });

    // Find if there is at least one field containing a "relation_list" type
    let relation_list_field = &operation_rows
        .iter()
        .find(|row| row.field_type == "relation_list");

    // If so, then parse the `relation_list` vec into an operation value and add it to the document view fields
    if let Some(relation_list_field) = relation_list_field {
        operation_fields
            .add(
                relation_list_field.name.as_str(),
                OperationValue::RelationList(RelationList::new(relation_list)),
            )
            .unwrap();
    }

    // Find if there is at least one field containing a "pinned_relation_list" type
    let pinned_relation_list_field = &operation_rows
        .iter()
        .find(|row| row.field_type == "pinned_relation_list");

    // If so, then parse the `pinned_relation_list` vec into an operation value and add it to the document view fields
    if let Some(pinned_relation_list_field) = pinned_relation_list_field {
        operation_fields
            .add(
                pinned_relation_list_field.name.as_str(),
                OperationValue::PinnedRelationList(PinnedRelationList::new(pinned_relation_list)),
            )
            .unwrap();
    }

    let operation = match first_row.action.as_str() {
        "create" => Operation::new_create(schema, operation_fields),
        "update" => Operation::new_update(schema, previous_operations, operation_fields),
        "delete" => Operation::new_delete(schema, previous_operations),
        _ => panic!("Operation which was not CREATE, UPDATE or DELETE found."),
    }
    // Unwrap as we are sure values coming from the db are validated
    .unwrap();

    Some(OperationStorage::new(
        &author,
        &operation,
        &operation_id,
        &document_id,
    ))
}

/// Takes a single `OperationValue` and parses it into a vector of string values.
///
/// OperationValues are inserted into the database as strings. If a value is a list
/// type (`RelationList` & `PinnedRelationList`) we insert one row for each value.
/// This method transforms a single operation into a list of string values, if the
/// is not a list, it will only contain a single item.
pub fn parse_value_to_string_vec(value: &OperationValue) -> Vec<String> {
    match value {
        OperationValue::Boolean(bool) => vec![bool.to_string()],
        OperationValue::Integer(int) => vec![int.to_string()],
        OperationValue::Float(float) => vec![float.to_string()],
        OperationValue::Text(str) => vec![str.to_string()],
        OperationValue::Relation(relation) => {
            vec![relation.document_id().as_str().to_string()]
        }
        OperationValue::RelationList(relation_list) => {
            let mut db_values = Vec::new();
            for document_id in relation_list.iter() {
                db_values.push(document_id.as_str().to_string())
            }
            db_values
        }
        OperationValue::PinnedRelation(pinned_relation) => {
            vec![pinned_relation.view_id().as_str()]
        }
        OperationValue::PinnedRelationList(pinned_relation_list) => pinned_relation_list
            .iter()
            .map(|document_view_id| document_view_id.as_str())
            .collect(),
    }
}

#[cfg(test)]
mod tests {

    use p2panda_rs::operation::{
        AsOperation, OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
    };

    use crate::db::models::operation::OperationFieldsJoinedRow;
    use crate::db::stores::test_utils::test_create_operation;
    use crate::db::traits::AsStorageOperation;

    use super::{parse_operation_rows, parse_value_to_string_vec};

    #[test]
    fn parses_operation_rows() {
        let operation_rows = vec![
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "age".to_string(),
                field_type: "int".to_string(),
                value: "28".to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "height".to_string(),
                field_type: "float".to_string(),
                value: "3.5".to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "is_admin".to_string(),
                field_type: "bool".to_string(),
                value: "false".to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "many_profile_pictures".to_string(),
                field_type: "relation_list".to_string(),
                value: "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "many_profile_pictures".to_string(),
                field_type: "relation_list".to_string(),
                value: "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "many_special_profile_pictures".to_string(),
                field_type: "pinned_relation_list".to_string(),
                value: "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "many_special_profile_pictures".to_string(),
                field_type: "pinned_relation_list".to_string(),
                value: "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "profile_picture".to_string(),
                field_type: "relation".to_string(),
                value: "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "special_profile_picture".to_string(),
                field_type: "pinned_relation".to_string(),
                value: "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .to_string(),
            },
            OperationFieldsJoinedRow {
                author: "2f8e50c2ede6d936ecc3144187ff1c273808185cfbc5ff3d3748d1ff7353fc96"
                    .to_string(),
                document_id: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                operation_id:
                    "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                        .to_string(),
                action: "create".to_string(),
                entry_hash: "0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543"
                    .to_string(),
                schema_id:
                    "venue_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
                        .to_string(),
                previous_operations: "".to_string(),
                name: "username".to_string(),
                field_type: "str".to_string(),
                value: "bubu".to_string(),
            },
        ];

        let operation = parse_operation_rows(operation_rows).unwrap();

        assert_eq!(
            operation.fields().unwrap().get("username").unwrap(),
            &OperationValue::Text("bubu".to_string())
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
                "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .parse()
                    .unwrap(),
                "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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
        )
    }

    #[test]
    fn operation_values_to_string_vec() {
        let expected_list = vec![
            "28",
            "3.5",
            "false",
            "0020aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0020bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "0020cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "0020dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "0020eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "0020ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "bubu",
        ];
        let mut string_value_list = vec![];
        let operation = test_create_operation();
        for (_, value) in operation.fields().unwrap().iter() {
            string_value_list.push(parse_value_to_string_vec(value));
        }
        let string_value_list: Vec<&String> = string_value_list.iter().flatten().collect();
        assert_eq!(expected_list, string_value_list)
    }
}
