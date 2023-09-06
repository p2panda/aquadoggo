// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::identity::PublicKey;
use sqlx::{FromRow, Type};

/// Resulting row of a custom SQL query for filtered, ordered and paginated collection requests
/// against the database.
///
/// Some fields will be set to default values when they have not been included in the SQL query.
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

    /// Unique identifier aiding cursor-based pagination.
    ///
    /// This value gets especially useful if we need to paginate over duplicate documents.
    pub cmp_value_cursor: String,

    #[sqlx(default)]
    pub root_cursor: String,

    /// Flag if document was at least changed once (more than one operation).
    #[sqlx(default)]
    pub is_edited: bool,

    /// Public key of original author of document / CREATE operation.
    #[sqlx(default)]
    pub owner: OptionalOwner,

    /// Name of the application field value we're interested in.
    #[sqlx(default)]
    pub name: String,

    /// Application field value.
    #[sqlx(default)]
    pub value: Option<String>,

    /// Data field used to store blob_piece_v1 bytes
    #[sqlx(default)]
    pub data: Option<Vec<u8>>,

    /// Type of the application field (string, boolean, float, int, relation etc.).
    #[sqlx(default)]
    pub field_type: String,

    /// Special index value for (pinned) relation list fields which can contain multiple values.
    /// The order of these values is crucial and needs to be preserved by keeping the index around.
    #[sqlx(default)]
    pub list_index: i32,
}

/// Wrapper around a public key, represented as a string in the database.
///
/// Since other structs assume that there is always a public key set, we have this wrapper type to
/// give a default "fake" public key, even when it was not returned by the SQL query.
#[derive(Type, Debug, Clone)]
#[sqlx(transparent)]
pub struct OptionalOwner(String);

impl From<&OptionalOwner> for PublicKey {
    fn from(value: &OptionalOwner) -> Self {
        // Unwrap here since we assume that public key was already validated before it was stored
        // in database
        value.0.parse().unwrap()
    }
}

impl Default for OptionalOwner {
    fn default() -> Self {
        // "Fake" ed25519 public key serving as a placeholder when it was not requested in query
        Self("0000000000000000000000000000000000000000000000000000000000000000".to_string())
    }
}
