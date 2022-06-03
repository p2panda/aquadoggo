// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a single row with joins from the document_view_fields table.
#[derive(FromRow, Debug, Clone)]
pub struct DocumentViewFieldRow {
    /// The id of this operation.
    pub document_view_id: String,

    /// The id of this operation.
    pub operation_id: String,

    /// The name of this field.
    pub name: String,

    /// The type of this field.
    pub field_type: String,

    /// The actual value contained in this field.
    pub value: String,
}
