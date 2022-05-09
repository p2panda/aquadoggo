// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::operation::{
    OperationFields, OperationValue, PinnedRelation, PinnedRelationList, Relation, RelationList,
};

use crate::db::models::operation::OperationFieldRow;

pub fn parse_operation_fields(operation_field_rows: Vec<OperationFieldRow>) -> OperationFields {
    let mut relation_list: Vec<DocumentId> = Vec::new();
    let mut pinned_relation_list: Vec<DocumentViewId> = Vec::new();

    let mut operation_fields = OperationFields::new();

    // Iterate over returned field values, for each value:
    //  - if it is a simple value type, parse it into an OperationValue and add it to the operation_fields
    //  - if it is a relation list value type parse each item into a DocumentId/DocumentViewId and push to
    //    the suitable vec (instantiated above)
    operation_field_rows.iter().for_each(|row| {
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
    let relation_list_field = &operation_field_rows
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
    let pinned_relation_list_field = &operation_field_rows
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

    operation_fields
}
