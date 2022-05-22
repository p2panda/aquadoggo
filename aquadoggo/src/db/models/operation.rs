// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a single operation row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationRow {
    /// The author of this operation.
    pub author: String,

    /// The id of the document this operation is part of.
    pub document_id: String,

    /// The id of this operation.
    pub operation_id: String,

    /// The action type this operation is performing.
    pub action: String,

    /// The hash of the entry this operation is associated with.
    pub entry_hash: String,

    /// The id of the schema this operation follows.
    pub schema_id: String,

    /// The previous operations of this operation concatenated into string format with `_` seperator.
    pub previous_operations: String,
}

/// A struct representing a single operation field row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationFieldRow {
    /// The id of the operation this field was published on.
    pub operation_id: String,

    /// The name of this field.
    pub name: String,

    /// The type of this field.
    pub field_type: String,

    /// The actual value contained in this field.
    pub value: String,
}

/// A struct representing a joined OperationRow and OperationFieldRow.
#[derive(FromRow, Debug, Clone)]
pub struct OperationFieldsJoinedRow {
    /// The author of this operation.
    pub author: String,

    /// The id of the document this operation is part of.
    pub document_id: String,

    /// The id of this operation.
    pub operation_id: String,

    /// The action type this operation is performing.
    pub action: String,

    /// The hash of the entry this operation is associated with.
    pub entry_hash: String,

    /// The id of the schema this operation follows.
    pub schema_id: String,

    /// The previous operations of this operation concatenated into string format with `_` seperator.
    pub previous_operations: String,

    /// The name of this field.
    pub name: String,

    /// The type of this field.
    pub field_type: String,

    /// The actual value contained in this field.
    pub value: String,
}
