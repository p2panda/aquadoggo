// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a single row with joins from the document_view_fields table.
#[derive(FromRow, Debug, Clone)]
pub struct DocumentViewFieldRow {
    /// Id of the document.
    pub document_id: String,

    /// Id of the view id representing the document version with this field value.
    pub document_view_id: String,

    /// Id of operation which set that field value.
    pub operation_id: String,

    /// Name of field.
    pub name: String,

    /// Position index of value when it is in a relation list.
    pub list_index: i32,

    /// Type of field.
    pub field_type: String,

    /// Actual value contained in field.
    /// This is optional as row representing an empty relation list has no value.
    pub value: Option<String>,

    /// Data field used to store blob_piece_v1 bytes
    pub data: Option<Vec<u8>>,
}

/// A struct representing a single row of a document table.
#[derive(FromRow, Debug, Clone)]
pub struct DocumentRow {
    /// Id of this document
    pub document_id: String,

    /// Id of this documents most recent view.
    pub document_view_id: String,

    /// Id of this documents schema.
    pub schema_id: String,

    /// Author of this document.
    pub public_key: String,

    /// Flag for if this document is deleted.
    pub is_deleted: bool,
}
