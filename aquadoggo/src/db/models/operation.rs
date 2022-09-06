// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a single operation row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationRow {
    /// The public key of the author of this operation.
    pub public_key: String,

    /// The id of the document this operation is part of.
    pub document_id: String,

    /// The id of this operation.
    ///
    /// Also, the hash of the entry this operation is associated with.
    pub operation_id: String,

    /// The action type this operation is performing.
    pub action: String,

    /// The id of the schema this operation follows.
    pub schema_id: String,

    /// The previous operations of this operation concatenated into string format with `_`
    /// separator.
    pub previous: Option<String>,
}

/// A struct representing a single operation field row as it is inserted in the database.
#[derive(FromRow, Debug)]
pub struct OperationFieldRow {
    /// The id of the operation this field was published on.
    pub operation_id: String,

    /// The name of this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub name: Option<String>,

    /// The type of this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub field_type: Option<String>,

    /// The actual value contained in this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub value: Option<String>,
}

/// A struct representing a joined OperationRow and OperationFieldRow.
#[derive(FromRow, Debug, Clone)]
pub struct OperationFieldsJoinedRow {
    /// The author of this operation.
    pub public_key: String,

    /// The id of the document this operation is part of.
    pub document_id: String,

    /// The id of this operation.
    pub operation_id: String,

    /// The action type this operation is performing.
    pub action: String,

    /// The id of the schema this operation follows.
    pub schema_id: String,

    /// The previous operations of this operation concatenated into string format with `_`
    /// separator.
    pub previous: Option<String>,

    /// The name of this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub name: Option<String>,

    /// The type of this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub field_type: Option<String>,

    /// The actual value contained in this field.
    ///
    /// This is an Option as a DELETE operation contains no fields.
    pub value: Option<String>,
}
