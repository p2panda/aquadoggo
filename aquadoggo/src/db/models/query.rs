// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// Resulting row of a custom SQL query for filtered, ordered and paginated collection requests
/// against the database.
#[derive(FromRow, Debug, Clone)]
pub struct QueryRow {
    /// Id of the document the operation is part of.
    pub document_id: String,

    /// Latest view id of the document.
    pub document_view_id: String,

    /// Id of the operation which contains that field value.
    pub operation_id: String,

    /// Flag if document is deleted.
    pub is_deleted: bool,

    /// Flag if document was at least changed once (more than one operation).
    pub is_edited: bool,

    /// Public key of original author of document / CREATE operation.
    pub owner: String,

    /// Name of the application field value we're interested in.
    pub name: String,

    /// Application field value.
    pub value: String,

    /// Type of the application field (string, boolean, float, int, relation etc.).
    pub field_type: String,

    /// Special index value for (pinned) relation list fields which can contain multiple values.
    /// The order of these values is crucial and needs to be preserved by keeping the index around.
    pub list_index: i32,
}
