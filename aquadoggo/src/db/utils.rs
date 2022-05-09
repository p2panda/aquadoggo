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
    // Unwrap as we know all possible strings should have been accounted for.
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
            // A special case, this is a list item, so we push it to a vec but _don't_ add it
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
            // A special case, this is a list item, so we push it to a vec but _don't_ add it
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
