// SPDX-License-Identifier: AGPL-3.0-or-later
pub struct Operation {
    id: String,
    author: String,
    entry_hash: String,
    schema_id_hash: String,
}

pub struct PreviousOperationRelation {
    parent_operation_id: String,
    child_operation_id: String,
}

pub struct OperationField {
    operation_id: String,
    name: String,
    field_type: String,
    value: String,
    relation_document_id: String,
    relation_document_view_id_hash: String,
}
