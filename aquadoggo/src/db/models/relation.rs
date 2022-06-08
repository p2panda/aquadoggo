// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a single row with joins from the document_view_fields table.
#[derive(FromRow, Debug, Clone)]
pub struct RelationRow {
    /// The type of this field.
    pub relation_type: String,

    /// The actual value contained in this field.
    pub value: String,
}
