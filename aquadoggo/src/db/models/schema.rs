// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// A struct representing a joined schema and schema field row in the database.
#[derive(FromRow, Debug, Clone)]
pub struct SchemaFieldRow {
    /// The id of this schema.
    pub schema_id: String,

    /// The name of this schema.
    pub name: String,

    /// The description of this schema.
    pub description: String,

    // The key for this field
    pub field_key: String,

    /// The type of this field.
    pub field_type: String,
}
